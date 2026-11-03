"""Exercise the actual post-publish workflow step with offline HTTP fixtures."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('release_rehearsal', ROOT/'scripts/report-release-rehearsal.py')
rehearsal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rehearsal)

STUB = r'''import json, os, pathlib, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
state = pathlib.Path(os.environ['CONFIRM_STATE'])
log = state/'calls.jsonl'
if name == 'sleep':
    with log.open('a') as out:
        out.write(json.dumps({'tool':'sleep','args':args})+'\n')
    raise SystemExit(0 if os.environ.get('CONFIRM_SLEEP') == 'continue' else 130)
if name != 'curl':
    raise SystemExit(97)
url = args[-1]
if url == 'https://crates.io/api/v1/crates/partitionline/'+os.environ['CONFIRM_VERSION']:
    endpoint = 'api'
elif url == 'https://index.crates.io/pa/rt/partitionline':
    endpoint = 'index'
else:
    raise SystemExit(97)
prior = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
n = sum(entry.get('endpoint') == endpoint for entry in prior)
routes = json.loads(os.environ['CONFIRM_ROUTES'])[endpoint]
response = routes[min(n, len(routes)-1)]
with log.open('a') as out:
    out.write(json.dumps({'tool':'curl','endpoint':endpoint,'response':response['code']})+'\n')
if '-o' not in args or '-w' not in args:
    raise SystemExit(97)
pathlib.Path(args[args.index('-o')+1]).write_text(response.get('body',''))
print(response['code'], end='')
raise SystemExit(response.get('exit', 0))
'''


def response(code, body='', exit_code=0):
    return {'code':str(code), 'body':body, 'exit':exit_code}


class ReleaseConfirmation(unittest.TestCase):
    def run_confirmation(self, api, index, *, skip='0', sleep='interrupt'):
        with tempfile.TemporaryDirectory(prefix='pl-confirmation-') as temp:
            state = Path(temp)
            for name in ['curl', 'sleep']:
                executable = state/name
                executable.write_text('#!'+sys.executable+'\n'+STUB)
                executable.chmod(0o755)
            version = rehearsal.candidate_version(ROOT)
            step = rehearsal.workflow_steps((ROOT/'.github/workflows/release.yml').read_text())['Confirm crates.io']
            step = step.replace('${{ steps.already.outputs.skip }}', skip)
            self.assertNotIn('${{', step)
            env = {'PATH':str(state)+':/usr/bin:/bin', 'LANG':'C.UTF-8',
                   'CONFIRM_STATE':str(state), 'CONFIRM_VERSION':version, 'CONFIRM_SLEEP':sleep,
                   'CONFIRM_ROUTES':json.dumps({'api':api, 'index':index}),
                   'PYTHONDONTWRITEBYTECODE':'1', 'TMPDIR':str(state)}
            proc = subprocess.run(['bash','-euo','pipefail','-c',step], cwd=ROOT, env=env,
                                  text=True,capture_output=True,timeout=60)
            log = state/'calls.jsonl'
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            return proc, calls

    def row(self, yanked=False):
        return json.dumps({'name':'partitionline','vers':rehearsal.candidate_version(ROOT),'yanked':yanked})+'\n'

    def api(self, *, crate='partitionline', version=None, yanked=False):
        return json.dumps({'version':{'crate':crate,
                           'num':version or rehearsal.candidate_version(ROOT),
                           'yanked':yanked}})+'\n'

    def test_live_and_skipped_paths_both_require_index_confirmation(self):
        for skip in ['0','1']:
            with self.subTest(skip=skip):
                proc,calls=self.run_confirmation([response(200,self.api())], [response(200,self.row())], skip=skip)
                self.assertEqual(proc.returncode,0,proc.stderr)
                self.assertIn('crates.io has partitionline',proc.stdout)
                self.assertEqual([x.get('endpoint') for x in calls],['api','index'])
                self.assertEqual('skipped publish' in proc.stdout,skip == '1')

    def test_index_visible_while_api_unavailable_is_not_confirmation(self):
        proc,calls=self.run_confirmation([response(503)], [response(200,self.row())])
        self.assertEqual(proc.returncode,130,proc.stderr)
        self.assertNotIn('crates.io has partitionline',proc.stdout)
        self.assertEqual([x.get('endpoint') for x in calls if x['tool']=='curl'],['api','index'])

    def test_api_only_redirect_index_failure_or_yank_never_confirm(self):
        major,minor,patch = rehearsal.candidate_version(ROOT).split('.')
        wrong_version = f'{major}.{minor}.{int(patch)+1}'
        cases = [
            ('api-only', response(200,self.api()), response(404)),
            ('api-redirect', response(302), response(503)),
            ('api-redirect-index-present', response(302), response(200,self.row())),
            ('api-404-index-present', response(404), response(200,self.row())),
            ('index-unavailable', response(200,self.api()), response(503)),
            ('yanked-index', response(200,self.api()), response(200,self.row(yanked=True))),
            ('yanked-api', response(200,self.api(yanked=True)), response(200,self.row())),
            ('wrong-api-crate', response(200,self.api(crate='other')), response(200,self.row())),
            ('wrong-api-version', response(200,self.api(version=wrong_version)), response(200,self.row())),
            ('missing-api-yanked', response(200,json.dumps({'version':{'crate':'partitionline',
                                  'num':rehearsal.candidate_version(ROOT)}})), response(200,self.row())),
            ('duplicate-api-key', response(200,'{"version":{},'+self.api()[1:]), response(200,self.row())),
            ('empty-api', response(200), response(200,self.row())),
            ('api-transfer-timeout', response(200,self.api(),exit_code=28), response(200,self.row())),
            ('malformed', response(404), response(200,'not-json\n')),
        ]
        for constant in ['NaN', 'Infinity', '-Infinity']:
            invalid_api = self.api().rstrip()[:-1]+',"invalid":'+constant+'}\n'
            invalid_index = self.row().rstrip()[:-1]+',"invalid":'+constant+'}\n'
            cases.append(('api-'+constant,response(200,invalid_api),response(200,self.row())))
            cases.append(('index-'+constant,response(200,self.api()),response(200,invalid_index)))
        for skip in ['0','1']:
            for label,api,index in cases:
                with self.subTest(skip=skip, case=label):
                    proc,calls=self.run_confirmation([api], [index], skip=skip)
                    self.assertEqual(proc.returncode,130,proc.stderr)
                    self.assertIn('waiting for crates.io index',proc.stdout)
                    self.assertNotIn('crates.io has partitionline',proc.stdout)
                    self.assertEqual([x.get('endpoint') for x in calls if x['tool']=='curl'],['api','index'])

    def test_index_can_appear_after_a_retry(self):
        proc,calls=self.run_confirmation([response(200,self.api())],
            [response(404),response(200,self.row())],sleep='continue')
        self.assertEqual(proc.returncode,0,proc.stderr)
        self.assertIn('waiting for crates.io index',proc.stdout)
        self.assertIn('crates.io has partitionline',proc.stdout)
        self.assertEqual([x['tool'] for x in calls],['curl','curl','sleep','curl','curl'])

    def test_unconfirmed_states_exhaust_bounded_attempts(self):
        cases = [
            ('api-only', response(200,self.api()), response(404), '0'),
            ('redirect', response(302), response(200,self.row()), '0'),
            ('yanked-index', response(200,self.api()), response(200,self.row(yanked=True)), '0'),
            ('timed-out-after-skip', response(200,self.api(),exit_code=28), response(200,self.row()), '1'),
        ]
        for name,api,index,skip in cases:
            with self.subTest(case=name):
                proc,calls=self.run_confirmation([api],[index],skip=skip,sleep='continue')
                self.assertEqual(proc.returncode,1,proc.stderr)
                self.assertIn('not confirmed by the API and sparse index',proc.stderr)
                self.assertNotIn('crates.io has partitionline',proc.stdout)
                self.assertEqual(sum(x['tool']=='curl' for x in calls),60)
                self.assertEqual(sum(x['tool']=='sleep' for x in calls),30)


if __name__ == '__main__':
    unittest.main()

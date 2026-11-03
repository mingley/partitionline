"""Offline controls for exact live CI run, attempt, jobs and artifact binding."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SHA = 'a' * 40
OTHER = 'b' * 40
RUN = 123
ATTEMPT = 2
JOBS = ['resource-soak', 'fmt', 'clippy', 'docs', 'schema', 'init-producer-id', 'zstd-decode',
        'release-rehearsal', 'test (stable)', 'macos-arm64 (stable)', 'windows (stable)', 'audit',
        'deny', 'alloc-budget', 'perf-gate', 'package', 'features', 'fuzz-smoke',
        'broker-smoke (apache/kafka:3.9.1)', 'broker-smoke (apache/kafka:4.1.0)',
        'broker-current (4.1.2)', 'broker-current (4.2.1)', 'broker-current (4.3.1)',
        'latency-gate', 'auth-smoke', 'integrity-smoke', 'conformance-fixtures']

FAKE_GH = r'''import json, os, pathlib, sys
args = sys.argv[1:]
data = json.loads(pathlib.Path(os.environ['TEST_CI_DATA']).read_text())
with open(os.environ['TEST_CI_LOG'], 'a') as f:
    f.write(json.dumps(args)+'\n')
if os.environ.get('GH_REPO') or os.environ.get('GH_HOST') != 'github.com':
    raise SystemExit(95)
if args[:2] == ['run','list']:
    if args[args.index('--repo')+1] != 'github.com/example/partitionline': raise SystemExit(95)
    fields = args[args.index('--json')+1].split(',')
    if 'attempt' in fields: raise SystemExit(98)
    marker = pathlib.Path(os.environ['TEST_CI_LIST_MARKER'])
    runs = data.get('runs_after', data['runs']) if marker.exists() else data['runs']
    marker.write_text('read')
    print(json.dumps(runs))
elif args[:2] == ['repo','view']:
    print('example/partitionline')
elif args[:1] == ['api']:
    if args[args.index('--hostname')+1] != 'github.com': raise SystemExit(95)
    endpoint = next((arg for arg in args if arg.startswith('repos/')), '')
    if '/attempts/' in endpoint and endpoint.endswith('/jobs?per_page=100'):
        key = 'jobs'
    elif endpoint.endswith('/artifacts?per_page=100'):
        key = 'artifacts'
    elif endpoint.endswith('/actions/runs/123'):
        marker = pathlib.Path(os.environ['TEST_CI_MARKER'])
        key = 'after' if marker.exists() else 'before'
        marker.write_text('read')
    else:
        raise SystemExit(97)
    if key in data.get('fail', []): raise SystemExit(1)
    if key in ('jobs','artifacts') and '--paginate' not in args: raise SystemExit(96)
    if key in data.get('raw', {}):
        print(data['raw'][key])
    elif key in ('jobs','artifacts'):
        for page in data[key]: print(json.dumps(page))
    else:
        print(json.dumps(data[key]))
else:
    raise SystemExit(97)
'''

FAKE_GIT = r'''import os, sys
args = sys.argv[1:]
if args[:1] == ['fetch']: raise SystemExit(0)
if args == ['remote','get-url','origin']:
    print(os.environ.get('TEST_ORIGIN_URL','https://github.com/example/partitionline.git')); raise SystemExit(0)
if args[:3] == ['rev-parse','-q','--verify']:
    print(os.environ['TEST_CI_SHA']); raise SystemExit(0)
raise SystemExit(97)
'''


def fixture():
    run = {'databaseId': RUN, 'headSha': SHA, 'name': 'ci', 'workflowName': 'ci',
           'status': 'completed', 'conclusion': 'success', 'createdAt': '2026-11-03T01:00:00Z'}
    meta = {'id': RUN, 'run_attempt': ATTEMPT, 'head_sha': SHA, 'head_branch': 'main',
            'event': 'push', 'path': '.github/workflows/ci.yml',
            'repository': {'full_name':'example/partitionline'},
            'head_repository': {'full_name':'example/partitionline'},
            'status': 'completed', 'conclusion': 'success'}
    jobs = [{'id': 10+i, 'run_id': RUN, 'run_attempt': ATTEMPT, 'head_sha': SHA,
             'name': name, 'status': 'completed', 'conclusion': 'success'}
            for i, name in enumerate(JOBS)]
    artifact = {'id': 900, 'name': f'conformance-fixture-artifacts-{RUN}-{ATTEMPT}',
                'expired': False, 'workflow_run': {'id': RUN, 'head_sha': SHA}}
    return {'runs': [run], 'before': meta, 'after': copy.deepcopy(meta),
            'jobs': [{'total_count': len(jobs), 'jobs': jobs}],
            'artifacts': [{'total_count': 1, 'artifacts': [artifact]}]}


class LiveCIEvidence(unittest.TestCase):
    def run_case(self, data, extra_env=None, spaced_tmp=False):
        with tempfile.TemporaryDirectory(prefix='pl-live-ci-offline-') as directory:
            folder = Path(directory)
            bin_dir = folder/'bin'
            bin_dir.mkdir()
            for name, code in [('gh', FAKE_GH), ('git', FAKE_GIT)]:
                path = bin_dir/name
                path.write_text('#!'+sys.executable+'\n'+code)
                path.chmod(0o755)
            data_path = folder/'data.json'
            data_path.write_text(json.dumps(data))
            log = folder/'calls.jsonl'
            temporary = folder
            sentinel = folder/'sentinel'
            if spaced_tmp:
                sentinel.write_text('do not delete')
                temporary = folder/'sentinel other'
                temporary.mkdir()
            env = {'PATH':str(bin_dir)+':/usr/bin:/bin', 'LANG':'C.UTF-8',
                   'PYTHONDONTWRITEBYTECODE':'1', 'CHECK_SHA':SHA, 'REQUIRE_MAIN_CI':'1',
                   'REQUIRE_PROFILE_EVIDENCE':'1', 'REQUIRED_JOBS':','.join(JOBS),
                   'TEST_CI_DATA':str(data_path), 'TEST_CI_LOG':str(log),
                   'TEST_CI_MARKER':str(folder/'marker'), 'TEST_CI_LIST_MARKER':str(folder/'list-marker'),
                   'TEST_CI_SHA':SHA, 'TMPDIR':str(temporary)}
            env.update(extra_env or {})
            result = subprocess.run(['bash','scripts/check-main-ci.sh'], cwd=ROOT, env=env,
                                    capture_output=True, text=True, timeout=30)
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            if spaced_tmp:
                self.assertEqual(sentinel.read_text(), 'do not delete')
                self.assertEqual(list(temporary.iterdir()), [])
            return result, calls

    def test_attempt_two_and_paginated_current_artifact(self):
        data = fixture()
        jobs = data['jobs'][0]['jobs']
        data['jobs'] = [{'total_count':len(jobs), 'jobs':jobs[:1]},
                        {'total_count':len(jobs), 'jobs':jobs[1:]}]
        current = data['artifacts'][0]['artifacts'][0]
        old = {**current, 'id':899, 'name':f'conformance-fixture-artifacts-{RUN}-1'}
        data['artifacts'] = [{'total_count':2, 'artifacts':[old]},
                             {'total_count':2, 'artifacts':[current]}]
        result, calls = self.run_case(data)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertIn('release profile complete',result.stdout)
        self.assertIn(['api', '--hostname', 'github.com', f'repos/example/partitionline/actions/runs/{RUN}/attempts/{ATTEMPT}/jobs?per_page=100', '--paginate'],calls)
        self.assertEqual(sum(c[:1] == ['api'] and f'repos/example/partitionline/actions/runs/{RUN}' in c for c in calls),2)
        self.assertFalse(any(c[:2] == ['run','view'] for c in calls))

    def test_live_run_metadata_must_match_exactly(self):
        for label,key,value in [('missing-attempt','run_attempt',None), ('zero-attempt','run_attempt',0),
                                ('boolean-attempt','run_attempt',True), ('string-attempt','run_attempt','2'),
                                ('wrong-id','id',999), ('short-sha','head_sha',SHA[:7]),
                                ('wrong-sha','head_sha',OTHER), ('wrong-branch','head_branch','topic'),
                                ('wrong-event','event','pull_request'), ('wrong-path','path','.github/workflows/other.yml'),
                                ('wrong-repo','repository',{'full_name':'other/partitionline'}),
                                ('missing-head-repo','head_repository',None),
                                ('pending','status','in_progress'), ('failed','conclusion','failure')]:
            with self.subTest(label=label):
                data=fixture()
                if value is None: data['before'].pop(key)
                else: data['before'][key]=value
                result,calls=self.run_case(data)
                self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
                self.assertFalse(any('/attempts/' in ' '.join(c) for c in calls))

    def test_changed_run_during_collection_is_rejected(self):
        for key,value in [('run_attempt',ATTEMPT+1),('head_sha',OTHER),('status','in_progress'),
                          ('conclusion','failure'),('repository',{'full_name':'other/partitionline'}),
                          ('head_repository',None)]:
            with self.subTest(key=key):
                data=fixture(); data['after'][key]=value
                result,calls=self.run_case(data)
                self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
                self.assertIn('[outcome=stale]',result.stderr)
                self.assertNotIn('release profile complete',result.stdout)

    def test_main_workflow_dispatch_can_qualify_with_full_profile(self):
        data=fixture()
        data['before']['event']='workflow_dispatch'
        data['after']['event']='workflow_dispatch'
        result,_=self.run_case(data)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)

    def test_jobs_must_be_complete_and_bound_to_the_attempt(self):
        for label,mutate in [
            ('wrong-attempt',lambda p:p[0]['jobs'][0].update(run_attempt=1)),
            ('missing-attempt',lambda p:p[0]['jobs'][0].pop('run_attempt')),
            ('wrong-run',lambda p:p[0]['jobs'][0].update(run_id=999)),
            ('string-run',lambda p:p[0]['jobs'][0].update(run_id=str(RUN))),
            ('short-sha',lambda p:p[0]['jobs'][0].update(head_sha=SHA[:7])),
            ('missing-sha',lambda p:p[0]['jobs'][0].pop('head_sha')),
            ('wrong-sha',lambda p:p[0]['jobs'][0].update(head_sha=OTHER)),
            ('pending',lambda p:p[0]['jobs'][0].update(status='in_progress')),
            ('skipped',lambda p:p[0]['jobs'][0].update(conclusion='skipped')),
            ('missing-page',lambda p:p[0].update(total_count=99)),
            ('missing-total',lambda p:p[0].pop('total_count')),
            ('duplicate-id',lambda p:p[0]['jobs'][1].update(id=p[0]['jobs'][0]['id'])),
            ('required-job-missing',lambda p:p[0]['jobs'][0].update(name='other')),
        ]:
            with self.subTest(label=label):
                data=fixture(); mutate(data['jobs'])
                result,_=self.run_case(data)
                self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)

    def test_artifacts_must_be_live_unique_and_for_this_run_and_attempt(self):
        for label,mutate in [
            ('prior-attempt',lambda a:a.update(name=f'conformance-fixture-artifacts-{RUN}-1')),
            ('legacy-name',lambda a:a.update(name='conformance-fixture-artifacts')),
            ('wrong-run',lambda a:a['workflow_run'].update(id=999)),
            ('string-run',lambda a:a['workflow_run'].update(id=str(RUN))),
            ('wrong-sha',lambda a:a['workflow_run'].update(head_sha=OTHER)),
            ('short-sha',lambda a:a['workflow_run'].update(head_sha=SHA[:7])),
            ('missing-sha',lambda a:a['workflow_run'].pop('head_sha')),
            ('expired',lambda a:a.update(expired=True)),
            ('unknown-expiry',lambda a:a.pop('expired')),
        ]:
            with self.subTest(label=label):
                data=fixture(); mutate(data['artifacts'][0]['artifacts'][0])
                result,_=self.run_case(data)
                self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
        data=fixture(); duplicate=copy.deepcopy(data['artifacts'][0]['artifacts'][0]); duplicate['id']=901
        data['artifacts'][0]['artifacts'].append(duplicate); data['artifacts'][0]['total_count']=2
        result,_=self.run_case(data)
        self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
        data=fixture(); data['artifacts'][0]['total_count']=2
        result,_=self.run_case(data)
        self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)

    def test_newer_same_sha_run_during_collection_is_rejected(self):
        data=fixture()
        newer={**data['runs'][0], 'databaseId':RUN+1, 'status':'queued',
               'conclusion':None, 'createdAt':'2026-11-03T02:00:00Z'}
        data['runs_after']=[newer, data['runs'][0]]
        result,_=self.run_case(data)
        self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertIn('[outcome=stale]',result.stderr)
        data=fixture(); data['runs_after']=[]
        result,_=self.run_case(data)
        self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
        data=fixture()
        unrelated={**data['runs'][0], 'databaseId':RUN+1, 'headSha':OTHER,
                   'createdAt':'2026-11-03T02:00:00Z'}
        data['runs_after']=[unrelated, data['runs'][0]]
        result,_=self.run_case(data)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        for other_id in [RUN-1, RUN+1]:
            with self.subTest(tied_id=other_id):
                data=fixture()
                tied={**data['runs'][0], 'databaseId':other_id, 'status':'queued', 'conclusion':None}
                data['runs_after']=[tied, data['runs'][0]]
                result,_=self.run_case(data)
                self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)

    def test_failed_or_malformed_live_fetches_never_fall_back(self):
        for key in ['before','jobs','artifacts','after']:
            with self.subTest(key=key):
                data=fixture(); data['fail']=[key]
                result,calls=self.run_case(data)
                self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
                self.assertFalse(any(c[:2] == ['run','view'] for c in calls))
        for key in ['before','jobs','artifacts','after']:
            with self.subTest(malformed=key):
                data=fixture(); data['raw']={key:'{broken'}
                result,_=self.run_case(data)
                self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
        for key,raw in [('before','{"run_attempt":1,"run_attempt":2}'),
                        ('jobs','{"total_count":0,"total_count":1,"jobs":[]}'),
                        ('artifacts','{"total_count":0,"artifacts":[],"bad":NaN}')]:
            with self.subTest(noncanonical=key):
                data=fixture(); data['raw']={key:raw}
                result,_=self.run_case(data)
                self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)

    def test_live_mode_cannot_reduce_profile_or_mix_fixture_evidence(self):
        data=fixture()
        jobs=data['jobs'][0]['jobs']
        jobs[:]=[job for job in jobs if job['name'] != 'broker-current (4.3.1)']
        data['jobs'][0]['total_count']=len(jobs)
        result,_=self.run_case(data, {'REQUIRED_JOBS':'fmt', 'REQUIRED_ARTIFACTS':'other'})
        self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertIn('broker-current (4.3.1)',result.stderr)
        for name in ['GH_JOBS_JSON','GH_ARTIFACTS_JSON']:
            with self.subTest(name=name):
                result,calls=self.run_case(fixture(), {name:'untrusted-fixture'})
                self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
                self.assertIn('fixture',result.stderr)
                self.assertFalse(any('/attempts/' in ' '.join(c) for c in calls))

    def test_origin_host_overrides_and_credentialed_urls_fail_closed(self):
        for url in ['https://github.com/example/partitionline.git',
                    'git@github.com:example/partitionline.git',
                    'ssh://git@github.com/example/partitionline.git']:
            with self.subTest(valid=url):
                result,_=self.run_case(fixture(), {'TEST_ORIGIN_URL':url, 'GH_REPO':'evil/other',
                                                 'GH_HOST':'evil.invalid', 'GITHUB_REPOSITORY':'example/partitionline'})
                self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        for url in ['https://fake.invalid/example/partitionline.git',
                    'https://test-secret@github.com/example/partitionline.git',
                    'https://github.com/example/partitionline.git?ref=main',
                    'git@github.com:example/partitionline/extra.git',
                    'https://github.com/example/../partitionline.git']:
            with self.subTest(invalid=url):
                result,calls=self.run_case(fixture(), {'TEST_ORIGIN_URL':url})
                self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
                self.assertNotIn('test-secret',result.stdout+result.stderr)
                self.assertFalse(calls)
        result,calls=self.run_case(fixture(), {'GITHUB_REPOSITORY':'other/partitionline'})
        self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertFalse(calls)

    def test_cleanup_preserves_sibling_when_tmpdir_contains_spaces(self):
        result,_=self.run_case(fixture(), spaced_tmp=True)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)

    def test_cleanup_on_early_run_selection_failure(self):
        data=fixture(); data['runs'][0]['headSha']=OTHER
        result,_=self.run_case(data, spaced_tmp=True)
        self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)

    def test_fixture_cleanup_never_deletes_inherited_tmp_variable(self):
        with tempfile.TemporaryDirectory(prefix='pl-fixture-cleanup-') as directory:
            folder=Path(directory)
            bindir=folder/'bin'; bindir.mkdir()
            git=bindir/'git'; git.write_text('#!'+sys.executable+'\n'+FAKE_GIT); git.chmod(0o755)
            sentinel=folder/'sentinel'; sentinel.write_text('do not delete')
            runs=folder/'runs.json'
            run=fixture()['runs'][0]; run['attempt']=ATTEMPT
            runs.write_text(json.dumps([run]))
            env={'PATH':str(bindir)+':/usr/bin:/bin', 'LANG':'C.UTF-8', 'CHECK_SHA':SHA,
                 'GH_RUNS_JSON':str(runs), 'REQUIRE_MAIN_CI':'1', 'TEST_CI_SHA':SHA,
                 'TMPDIR':directory, 'tmp':str(sentinel), 'PYTHONDONTWRITEBYTECODE':'1'}
            result=subprocess.run(['bash','scripts/check-main-ci.sh'],cwd=ROOT,env=env,
                                  capture_output=True,text=True,timeout=30)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            self.assertEqual(sentinel.read_text(),'do not delete')

    def test_self_test_handles_space_in_tmpdir(self):
        with tempfile.TemporaryDirectory(prefix='pl-self-test-space-') as directory:
            temporary=Path(directory)/'space in directory'; temporary.mkdir()
            env={'PATH':'/usr/bin:/bin', 'LANG':'C.UTF-8', 'TMPDIR':str(temporary),
                 'PYTHONDONTWRITEBYTECODE':'1'}
            result=subprocess.run(['bash','scripts/check-main-ci.sh','--self-test'], cwd=ROOT, env=env,
                                  capture_output=True,text=True,timeout=30)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            self.assertIn('self-test: OK',result.stdout)
            self.assertEqual(list(temporary.iterdir()),[])

    def test_self_test_does_not_execute_tmpdir_metacharacters(self):
        with tempfile.TemporaryDirectory(prefix='pl-self-test-metachar-') as directory:
            folder=Path(directory)
            sentinel=folder/'injected'
            temporary=folder/"before';touch${IFS}${MARKER};#"; temporary.mkdir()
            env={'PATH':'/usr/bin:/bin', 'LANG':'C.UTF-8', 'TMPDIR':str(temporary),
                 'MARKER':str(sentinel), 'PYTHONDONTWRITEBYTECODE':'1'}
            result=subprocess.run(['bash','scripts/check-main-ci.sh','--self-test'], cwd=ROOT, env=env,
                                  capture_output=True,text=True,timeout=30)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            self.assertIn('self-test: OK',result.stdout)
            self.assertFalse(sentinel.exists(), 'TMPDIR was executed as shell code')
            self.assertEqual(list(temporary.iterdir()),[])


if __name__ == '__main__':
    unittest.main()

"""Offline local-release controls using fake HTTP, Cargo and document flips."""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[2]

SHIM = r'''import json, os, pathlib, re, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
log = pathlib.Path(os.environ['TEST_CALLS'])
def record(**fields):
    with log.open('a') as out:
        out.write(json.dumps(fields)+'\n')
if name == 'curl':
    url = args[-1]
    version = os.environ['TEST_VERSION']
    api = 'https://crates.io/api/v1/crates/partitionline/'
    index = 'https://index.crates.io/pa/rt/partitionline'
    if url.startswith(api):
        requested = url[len(api):]
        after = pathlib.Path(os.environ['TEST_UPLOAD_MARKER']).exists()
        code = (os.environ.get('TEST_AFTER_API',os.environ['TEST_API']) if after else os.environ['TEST_API']) if requested == version else '404'
        body = json.dumps({'version':{'crate':'partitionline','num':version,
                                      'yanked':os.environ.get('TEST_API_YANKED') == '1'}})
        if os.environ.get('TEST_BAD_API') == '1': body = '{invalid'
        fail = os.environ.get('TEST_API_FAIL') == '1'
        endpoint = 'api'
    elif url == index:
        after = pathlib.Path(os.environ['TEST_UPLOAD_MARKER']).exists()
        code = os.environ.get('TEST_AFTER_INDEX',os.environ['TEST_INDEX']) if after else os.environ['TEST_INDEX']
        body = json.dumps({'name':'partitionline','vers':version,
                           'yanked':os.environ.get('TEST_INDEX_YANKED') == '1'})+'\n'
        if os.environ.get('TEST_BAD_INDEX') == '1': body = '{invalid\n'
        fail = os.environ.get('TEST_INDEX_FAIL') == '1'
        endpoint = 'index'
    else:
        raise SystemExit(97)
    if '-o' not in args or '-w' not in args: raise SystemExit(97)
    pathlib.Path(args[args.index('-o')+1]).write_text(body)
    record(tool='curl', endpoint=endpoint, url=url, code=code)
    print(code, end='')
    raise SystemExit(18 if fail else 0)
if name == 'cargo':
    manifest = pathlib.Path('Cargo.toml').read_text()
    m = re.search(r'^partitionline = "=([^"]+)"$', manifest, re.M)
    record(tool='cargo', args=args, mode=os.environ.get('MODE'), ver=os.environ.get('VER'),
           registry_version=m.group(1) if m else None)
    if args == ['publish']:
        pathlib.Path(os.environ['TEST_UPLOAD_MARKER']).write_text('simulated')
    if os.environ.get('TEST_CARGO_FAIL') == '1': raise SystemExit(23)
    if m:
        mode=os.environ.get('TEST_LOCK_MODE','registry')
        root_deps='["tokio"]' if mode == 'missing-direct' else '["partitionline", "tokio"]'
        lock='[[package]]\nname = "partitionline-crates-io-consumer"\nversion = "0.0.0"\ndependencies = '+root_deps+'\n'
        lock+='[[package]]\nname = "partitionline"\nversion = "'+m.group(1)+'"\n'
        if mode != 'path':
            source='registry+https://alternate.invalid/index' if mode == 'alternate' else 'registry+https://github.com/rust-lang/crates.io-index'
            lock+='source = "'+source+'"\n'
        if mode == 'duplicate':
            lock+='[[package]]\nname = "partitionline"\nversion = "'+m.group(1)+'"\n'
        lock+='[[package]]\nname = "tokio"\nversion = "1.0.0"\nsource = "registry+https://github.com/rust-lang/crates.io-index"\n'
        pathlib.Path('Cargo.lock').write_text(lock)
    git_dep=re.search(r'^partitionline = \{ git = "([^"]+)", tag = "([^"]+)" \}$',manifest,re.M)
    if git_dep:
        mode=os.environ.get('TEST_LOCK_MODE','git')
        root_deps='["tokio"]' if mode == 'missing-direct' else '["partitionline", "tokio"]'
        lock='[[package]]\nname = "partitionline-crates-io-consumer"\nversion = "0.0.0"\ndependencies = '+root_deps+'\n'
        lock+='[[package]]\nname = "partitionline"\nversion = "'+('0.9.0' if mode == 'wrong-version' else '0.1.0')+'"\n'
        repo='https://other.invalid/partitionline' if mode == 'wrong-repo' else git_dep.group(1)
        tag='v0.9.0' if mode == 'wrong-tag' else git_dep.group(2)
        commit='b'*40 if mode == 'wrong-commit' else 'a'*40
        if mode != 'path': lock+='source = "git+'+repo+'?tag='+tag+'#'+commit+'"\n'
        if mode == 'duplicate': lock+='[[package]]\nname = "partitionline"\nversion = "0.1.0"\n'
        lock+='[[package]]\nname = "tokio"\nversion = "1.0.0"\nsource = "git+https://example.invalid/tokio?tag=v1#'+'c'*40+'"\n'
        pathlib.Path('Cargo.lock').write_text(lock)
    raise SystemExit(0)
if name == 'git':
    record(tool='git', args=args)
    if args == ['rev-parse','--abbrev-ref','HEAD']: print('main')
    elif args[:1] == ['status']: pass
    elif args[:2] == ['rev-parse','HEAD']: print('1'*40)
    elif args == ['remote','get-url','origin']: print('https://github.com/example/partitionline.git')
    elif args[:3] == ['rev-parse','-q','--verify'] and args[-1].startswith('refs/tags/'): pass
    elif args[:2] == ['rev-parse','--verify'] and args[-1].startswith('refs/tags/'): print('a'*40)
    elif args[:1] == ['show'] and args[-1] == 'a'*40+':Cargo.toml': print('[package]\nname = "partitionline"\nversion = "0.1.0"')
    else: raise SystemExit(97)
    raise SystemExit(0)
if name == 'gh':
    record(tool='gh', args=args)
    if args[:2] == ['workflow','view']:
        print('offline workflow fixture')
        raise SystemExit(0)
    raise SystemExit(97)
if name == 'sleep':
    record(tool='sleep', args=args)
    raise SystemExit(0)
raise SystemExit(97)
'''


class LocalConfirmation(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='pl-local-confirm-')
        self.base = Path(self.temp.name)
        self.root = self.base/'source'
        (self.root/'scripts/lib').mkdir(parents=True)
        for path in ['Cargo.toml','scripts/check-installable.sh','scripts/lib/crates-io.sh',
                     'scripts/lib/cargo-registry-token.sh','scripts/lib/adopter-consumer-main.sh',
                     'scripts/owner-publish.sh','scripts/owner-dispatch-first-publish.sh',
                     'scripts/verify-crates-io-consumer.sh',
                     'scripts/day1-after-publish.sh']:
            dest=self.root/path
            dest.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(ROOT/path,dest)
        for script in ['post-publish-readme.sh','post-publish-adoption.sh',
                       'post-publish-guide.sh','post-publish-migrate.sh']:
            (self.root/'scripts'/script).write_text('echo "flip '+script+'" >> "$TEST_FLIPS"\n')
        (self.root/'scripts/check-crates-io-description.sh').write_text('echo description-stub\n')
        manifest=self.root/'Cargo.toml'
        original=manifest.read_text()
        current=tomllib.loads(original)['package']['version']
        pattern=r'(?m)^version = "'+re.escape(current)+r'"$'
        self.assertEqual(len(re.findall(pattern,original)),1)
        manifest.write_text(re.sub(pattern,'version = "0.2.0"',original,count=1))
        bindir=self.base/'bin'
        bindir.mkdir()
        for name in ['curl','cargo','git','gh','sleep']:
            path=bindir/name
            path.write_text('#!'+sys.executable+'\n'+SHIM)
            path.chmod(0o755)
        self.calls=self.base/'calls.jsonl'
        self.flips=self.base/'flips.log'
        self.env={'PATH':str(bindir)+':/usr/bin:/bin','LANG':'C.UTF-8',
                  'TEST_CALLS':str(self.calls),'TEST_FLIPS':str(self.flips),
                  'TEST_UPLOAD_MARKER':str(self.base/'simulated-upload'),
                  'TEST_VERSION':'0.2.0','TEST_API':'200','TEST_INDEX':'200',
                  'TMPDIR':str(self.base),'PYTHONDONTWRITEBYTECODE':'1',
                  'CARGO_CHECK_ATTEMPTS':'1','WAIT_CRATES_ATTEMPTS':'1','ATTEMPTS':'1','SLEEP_SECS':'0'}

    def tearDown(self):
        self.temp.cleanup()

    def run_script(self, path, *args, env=None):
        actual={**self.env,**(env or {})}
        result=subprocess.run(['bash',path,*args], cwd=self.root, env=actual,
                              capture_output=True,text=True,timeout=60)
        calls=[json.loads(s) for s in self.calls.read_text().splitlines()] if self.calls.exists() else []
        return result,calls

    def test_dual_source_confirmation_is_required(self):
        cases=[('both',{},0), ('api-down',{'TEST_API':'503'},1),
               ('api-404',{'TEST_API':'404'},1), ('api-redirect',{'TEST_API':'302'},1),
               ('api-timeout',{'TEST_API_FAIL':'1'},1), ('api-malformed',{'TEST_BAD_API':'1'},1),
               ('api-yanked',{'TEST_API_YANKED':'1'},1), ('index-down',{'TEST_INDEX':'503'},1),
               ('index-404',{'TEST_INDEX':'404'},1), ('both-404',{'TEST_API':'404','TEST_INDEX':'404'},3),
               ('index-timeout',{'TEST_INDEX_FAIL':'1'},1),
               ('index-malformed',{'TEST_BAD_INDEX':'1'},1), ('index-yanked',{'TEST_INDEX_YANKED':'1'},1)]
        for label,env,expected in cases:
            with self.subTest(label=label):
                result,calls=self.run_script('scripts/check-installable.sh',env=env)
                self.assertEqual(result.returncode,expected,result.stdout+result.stderr)
                self.assertFalse(any(x['tool']=='cargo' for x in calls))
                if expected == 0:
                    self.assertIn('external consumer',result.stdout.lower())

    def test_exact_version_override_probes_the_requested_version(self):
        result,calls=self.run_script('scripts/verify-crates-io-consumer.sh',
                                     env={'MODE':'registry','VER':'0.1.0','TEST_VERSION':'0.1.0'})
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        api=[x['url'] for x in calls if x['tool']=='curl' and x['endpoint']=='api']
        self.assertTrue(api and all(x.endswith('/0.1.0') for x in api),api)
        cargo=[x for x in calls if x['tool']=='cargo']
        self.assertEqual([x['registry_version'] for x in cargo],['0.1.0'])

    def test_registry_consumer_rejects_wrong_or_ambiguous_lock_source(self):
        for mode in ['path','alternate','duplicate','missing-direct']:
            with self.subTest(mode=mode):
                result,calls=self.run_script('scripts/verify-crates-io-consumer.sh',
                                             env={'MODE':'registry','TEST_LOCK_MODE':mode})
                self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
                self.assertIn('invalid registry consumer lock',result.stderr)
                self.assertTrue(any(x['tool']=='cargo' for x in calls))

    def test_git_consumer_binds_tagged_version_url_commit_and_direct_dependency(self):
        (self.root/'README.md').write_text('partitionline = { git = "https://github.com/mingley/partitionline", tag = "v0.1.0" }\n')
        (self.root/'scripts/check-adopter-pin.sh').write_text('exit 0\n')
        for mode in ['git','path','wrong-repo','wrong-tag','wrong-commit','wrong-version','duplicate','missing-direct']:
            with self.subTest(mode=mode):
                self.calls.unlink(missing_ok=True)
                result,calls=self.run_script('scripts/verify-crates-io-consumer.sh',
                                             env={'MODE':'git','TEST_LOCK_MODE':mode})
                self.assertEqual(result.returncode,0 if mode == 'git' else 1,result.stdout+result.stderr)
                self.assertTrue(any(x['tool']=='cargo' for x in calls))
                if mode != 'git': self.assertIn('invalid git consumer lock',result.stderr)

    def test_audit_does_not_report_a_skipped_git_consumer_as_compiled(self):
        source=(ROOT/'scripts/audit-civilization-bars.sh').read_text()
        start=source.index('  if MODE=git bash scripts/verify-crates-io-consumer.sh')
        end=source.index('\nelse\n  bad "git-tag adopter consumer not wired',start)
        fragment=source[start:end].replace('/tmp/pl-git-adopter.log','"$TEST_CALLS.git-log"')
        (self.root/'scripts/audit-fragment.sh').write_text('ok() { echo "PASS $*"; }\nbad() { echo "FAIL $*"; }\n'+fragment+'\n')
        (self.root/'scripts/verify-crates-io-consumer.sh').write_text('echo "verify-crates-io-consumer: MODE=git SKIP — README already crates.io-shaped"\n')
        result,_=self.run_script('scripts/audit-fragment.sh')
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertIn('no git compile claimed',result.stdout)
        self.assertNotIn('cargo-checks',result.stdout)

    def test_day1_forces_registry_consumer_for_manifest_version(self):
        result,calls=self.run_script('scripts/day1-after-publish.sh',
                                     env={'MODE':'path','VER':'0.1.0'})
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        cargo=[x for x in calls if x['tool']=='cargo']
        self.assertEqual([x['registry_version'] for x in cargo],['0.2.0'])
        self.assertTrue(self.flips.is_file())

    def test_consumer_failure_stops_day1_before_doc_flips(self):
        result,calls=self.run_script('scripts/day1-after-publish.sh',env={'TEST_CARGO_FAIL':'1'})
        self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertTrue(any(x['tool']=='cargo' for x in calls))
        self.assertFalse(self.flips.exists())

    def test_day1_unknown_does_not_advice_publication(self):
        result,calls=self.run_script('scripts/day1-after-publish.sh',env={'TEST_API':'503'})
        self.assertEqual(result.returncode,1,result.stdout+result.stderr)
        self.assertIn('inconclusive',result.stderr.lower())
        self.assertIn('may already exist',result.stderr.lower())
        self.assertNotIn('publish first',result.stderr.lower())
        self.assertFalse(any(x['tool']=='cargo' for x in calls))
        self.assertFalse(self.flips.exists())

    def test_existing_index_entry_skips_duplicate_publication_during_api_outage(self):
        env={'TEST_API':'503','CARGO_REGISTRY_TOKEN':'offline-test-placeholder',
             'RUN_DAY1_AFTER_PUBLISH':'0'}
        result,calls=self.run_script('scripts/owner-publish.sh',env=env)
        self.assertEqual(result.returncode,2,result.stdout+result.stderr)
        self.assertIn('skipping cargo publish',result.stdout)
        self.assertIn('PARTIAL',result.stderr)
        self.assertFalse(any(x['tool']=='cargo' for x in calls))

    def install_ci_stub(self):
        (self.root/'scripts/check-main-ci.sh').write_text('''python3 - <<'PY'
import json, os
keys = ['GH_RUNS_JSON','GH_JOBS_JSON','GH_ARTIFACTS_JSON','REQUIRED_JOBS',
        'REQUIRED_ARTIFACTS','GH_RUN_LIMIT','REQUIRE_MAIN_CI','REQUIRE_PROFILE_EVIDENCE',
        'MAIN_BRANCH','CHECK_SHA']
with open(os.environ['TEST_CALLS'], 'a') as f:
    f.write(json.dumps({'tool':'ci','env':{k:os.environ.get(k) for k in keys}})+'\\n')
PY
exit "${TEST_CI_EXIT:-0}"
''')
        (self.root/'scripts/ci-publish-ready.sh').write_text('exit 0\n')

    def test_owner_publish_fails_closed_for_every_nonzero_ci_status(self):
        self.install_ci_stub()
        for code in ['1','2','42','126','127']:
            with self.subTest(code=code):
                self.calls.unlink(missing_ok=True)
                env={'TEST_API':'404','TEST_INDEX':'404','CARGO_REGISTRY_TOKEN':'test-placeholder',
                     'RUN_DAY1_AFTER_PUBLISH':'0','TEST_CI_EXIT':code, 'ALLOW_RED_MAIN':'1',
                     'REQUIRE_MAIN_CI':'0','REQUIRE_PROFILE_EVIDENCE':'0',
                     'GH_RUNS_JSON':'fixture','GH_JOBS_JSON':'fixture','GH_ARTIFACTS_JSON':'fixture',
                     'REQUIRED_JOBS':'fake','REQUIRED_ARTIFACTS':'fake','GH_RUN_LIMIT':'1',
                     'MAIN_BRANCH':'fake'}
                result,calls=self.run_script('scripts/owner-publish.sh',env=env)
                self.assertEqual(result.returncode,1,result.stdout+result.stderr)
                self.assertFalse(any(x['tool']=='cargo' for x in calls),calls)
                ci=[x['env'] for x in calls if x['tool']=='ci']
                self.assertEqual(len(ci),1)
                self.assertEqual(ci[0]['REQUIRE_MAIN_CI'],'1')
                self.assertEqual(ci[0]['REQUIRE_PROFILE_EVIDENCE'],'1')
                self.assertEqual(ci[0]['MAIN_BRANCH'],'main')
                self.assertEqual(ci[0]['CHECK_SHA'],'1'*40)
                for name in ['GH_RUNS_JSON','GH_JOBS_JSON','GH_ARTIFACTS_JSON',
                             'REQUIRED_JOBS','REQUIRED_ARTIFACTS','GH_RUN_LIMIT']:
                    self.assertIsNone(ci[0][name])

    def test_publisher_confirms_both_sources_after_simulated_upload(self):
        self.install_ci_stub()
        for label,after,expected in [
            ('both',{'TEST_AFTER_API':'200','TEST_AFTER_INDEX':'200'},0),
            ('index-only',{'TEST_AFTER_API':'503','TEST_AFTER_INDEX':'200'},2),
            ('api-only',{'TEST_AFTER_API':'200','TEST_AFTER_INDEX':'404'},2),
            ('unknown',{'TEST_AFTER_API':'503','TEST_AFTER_INDEX':'503'},2),
        ]:
            with self.subTest(label=label):
                self.calls.unlink(missing_ok=True)
                Path(self.env['TEST_UPLOAD_MARKER']).unlink(missing_ok=True)
                env={'TEST_API':'404','TEST_INDEX':'404','CARGO_REGISTRY_TOKEN':'test-placeholder',
                     'RUN_DAY1_AFTER_PUBLISH':'0',**after}
                result,calls=self.run_script('scripts/owner-publish.sh',env=env)
                self.assertEqual(result.returncode,expected,result.stdout+result.stderr)
                self.assertEqual([x['args'] for x in calls if x['tool']=='cargo'],[['publish']])
                if expected == 0:
                    self.assertIn('external consumer and day1 handoff are outstanding',result.stdout)
                else:
                    self.assertIn('PARTIAL',result.stderr)

    def test_real_checker_cannot_inherit_fake_green_fixture(self):
        shutil.copyfile(ROOT/'scripts/check-main-ci.sh',self.root/'scripts/check-main-ci.sh')
        (self.root/'scripts/ci-publish-ready.sh').write_text('exit 0\n')
        fake=self.base/'fake-green.json'
        fake.write_text(json.dumps([{'databaseId':123,'headSha':'1'*40,'name':'CI',
                                     'status':'completed','conclusion':'success'}]))
        env={'TEST_API':'404','TEST_INDEX':'404','CARGO_REGISTRY_TOKEN':'test-placeholder',
             'RUN_DAY1_AFTER_PUBLISH':'0','REQUIRE_MAIN_CI':'1','GH_RUNS_JSON':str(fake),
             'REQUIRED_JOBS':'fake','REQUIRED_ARTIFACTS':'fake'}
        result,calls=self.run_script('scripts/owner-publish.sh',env=env)
        self.assertNotEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertTrue(any(x['tool']=='gh' and x['args'][:2]==['run','list'] for x in calls),calls)
        self.assertFalse(any(x['tool']=='cargo' for x in calls),calls)

    def test_other_real_ci_gates_clear_fixtures_and_fail_closed(self):
        self.install_ci_stub()
        cases=[('scripts/owner-cut-release.sh','release_ci_required=1',
                '\nif [[ "$SKIP_PUBLISH_READY" != "1" ]]',{'DRY_RUN':'0','REQUIRE_MAIN_CI':'0','ALLOW_RED_MAIN':'1'}),
               ('scripts/owner-finish-installable.sh','release_ci_required=1',
                '\nif [[ "$CANCEL_STUCK" == "1" ]]',{'DRY_RUN':'0','REQUIRE_MAIN_CI':'0','ALLOW_RED_MAIN':'1'}),
               ('scripts/ci-publish-ready.sh','ci_rc=0',
                '\necho "== publish dry-run =="',{})]
        for script,start,end,extra in cases:
            source=(ROOT/script).read_text()
            block=source[source.index(start,source.index('== exact-SHA') if 'owner-cut' in script or 'ci-publish' in script else source.index('== 2b)')):]
            block=block[:block.index(end)]
            (self.root/'scripts/ci-fragment.sh').write_text('set -euo pipefail\n'+block+'\n')
            for code in ['1','2','42','126','127']:
                with self.subTest(script=script,code=code):
                    self.calls.unlink(missing_ok=True)
                    env={'TEST_CI_EXIT':code,'GH_RUNS_JSON':'fixture','GH_JOBS_JSON':'fixture',
                         'GH_ARTIFACTS_JSON':'fixture','REQUIRED_JOBS':'fake','REQUIRED_ARTIFACTS':'fake',
                         'GH_RUN_LIMIT':'1','REQUIRE_PROFILE_EVIDENCE':'0','MAIN_BRANCH':'fake',
                         'CHECK_SHA':'fake',**extra}
                    result,calls=self.run_script('scripts/ci-fragment.sh',env=env)
                    self.assertEqual(result.returncode,1,result.stdout+result.stderr)
                    ci=[x['env'] for x in calls if x['tool']=='ci']
                    self.assertEqual(len(ci),1)
                    self.assertEqual(ci[0]['REQUIRE_MAIN_CI'],'1')
                    self.assertEqual(ci[0]['REQUIRE_PROFILE_EVIDENCE'],'1')
                    self.assertEqual(ci[0]['MAIN_BRANCH'],'main')
                    self.assertEqual(ci[0]['CHECK_SHA'], None if 'owner-finish' in script else '1'*40)
                    for name in ['GH_RUNS_JSON','GH_JOBS_JSON','GH_ARTIFACTS_JSON',
                                 'REQUIRED_JOBS','REQUIRED_ARTIFACTS','GH_RUN_LIMIT']:
                        self.assertIsNone(ci[0][name])

    def test_owner_sync_refuses_unexpected_ci_result_and_clears_fixtures(self):
        self.install_ci_stub()
        source=(ROOT/'scripts/owner-sync-main.sh').read_text()
        start=source.index('ci_rc=0',source.index('# Refuse to cancel'))
        end=source.index('\nif [[ "$DRY_RUN" == "1" ]]',start)
        (self.root/'scripts/ci-fragment.sh').write_text('set -euo pipefail\n'+source[start:end]+'\n')
        for code in ['42','126','127']:
            with self.subTest(code=code):
                self.calls.unlink(missing_ok=True)
                env={'TEST_CI_EXIT':code,'ALLOW_BUSY_MAIN':'1','CHECK_SHA':'fake',
                     'GH_RUNS_JSON':'fixture','GH_JOBS_JSON':'fixture','GH_ARTIFACTS_JSON':'fixture',
                     'REQUIRED_JOBS':'fake','REQUIRED_ARTIFACTS':'fake','GH_RUN_LIMIT':'1'}
                result,calls=self.run_script('scripts/ci-fragment.sh',env=env)
                self.assertEqual(result.returncode,1,result.stdout+result.stderr)
                ci=[x['env'] for x in calls if x['tool']=='ci']
                self.assertEqual(len(ci),1)
                self.assertEqual(ci[0]['MAIN_BRANCH'],'main')
                self.assertEqual(ci[0]['REQUIRE_PROFILE_EVIDENCE'],'1')
                for name in ['CHECK_SHA','GH_RUNS_JSON','GH_JOBS_JSON','GH_ARTIFACTS_JSON',
                             'REQUIRED_JOBS','REQUIRED_ARTIFACTS','GH_RUN_LIMIT']:
                    self.assertIsNone(ci[0][name])

    def test_retired_dispatcher_refuses_all_overrides_without_gh(self):
        for label,env,expected in [
            ('workflow-file',{'WORKFLOW':'first-publish.yml'},2),
            ('workflow-name',{'WORKFLOW':'First publish (crates.io) [RETIRED]'},2),
            ('workflow-id',{'WORKFLOW':'12345'},2),
            ('other-file',{'WORKFLOW':'release.yml','TEST_API':'404','TEST_INDEX':'404'},2),
            ('preview',{'WORKFLOW':'release.yml','DRY_RUN':'1'},2),
            ('retired-default',{},2),
        ]:
            with self.subTest(label=label):
                self.calls.unlink(missing_ok=True)
                result,calls=self.run_script('scripts/owner-dispatch-first-publish.sh',env=env)
                self.assertEqual(result.returncode,expected,result.stdout+result.stderr)
                self.assertIn('retired',result.stderr.lower())
                self.assertFalse(any(x['tool'] in ['gh','cargo','curl'] for x in calls),calls)

    def test_partial_summary_paths_do_not_treat_unknown_as_absent(self):
        for path in ['ci-publish-ready.sh','ci-branch-lite.sh','check-cut-path.sh']:
            text=(ROOT/'scripts'/path).read_text()
            start=text.index('if [[ "${day1_rc:-0}" -eq 2 ||')
            end=text.index('\nfi',start)+3
            command=('set -euo pipefail\nday1_rc=2\ndispatch_rc=0\nhandoff_rc=0\n'
                     'finish_rc=0\nver=0.2.0\n'+text[start:end])
            for label,env,expected in [('unknown',{'TEST_API':'503'},2),
                                       ('confirmed',{},2),
                                       ('absent',{'TEST_API':'404','TEST_INDEX':'404'},0),
                                       ('absent-with-token',{'TEST_API':'404','TEST_INDEX':'404',
                                                             'CARGO_REGISTRY_TOKEN':'offline-placeholder'},0)]:
                with self.subTest(path=path,state=label):
                    result=subprocess.run(['bash','-c',command],cwd=self.root,
                        env={**self.env,**env},capture_output=True,text=True,timeout=15)
                    self.assertEqual(result.returncode,expected,result.stdout+result.stderr)
                    if label.startswith('absent'):
                        self.assertIn('registry reports this version absent; external consumer remains pending',result.stdout)
                        self.assertNotIn('blocked on CARGO_REGISTRY_TOKEN',result.stdout)
                    if label == 'unknown':
                        self.assertIn('inconclusive',result.stderr.lower())
                        self.assertNotIn('pre-token rehearsal',result.stdout.lower())

    def test_preflight_and_finish_do_not_interpret_unknown_as_absent(self):
        for path in ['check-installable-preflight.sh','owner-finish-installable.sh']:
            text=(ROOT/'scripts'/path).read_text()
            if path == 'check-installable-preflight.sh':
                start=text.index('# Already published?')
                end=text.index('\necho\necho "== merge/tag readiness ==',start)
            else:
                start=text.index('echo "== 0) Already Installable? =="')
                end=text.index('\necho\necho "== 1) Token ==',start)
            fragment=text[start:end].replace('/tmp/pl-preflight-installable.log',str(self.base/'preflight.log'))
            command='set -euo pipefail\nname=partitionline\nver=0.2.0\n'+fragment
            unknown_code = 2 if path == 'owner-finish-installable.sh' else 1
            for label,env,code in [('unknown',{'TEST_API':'503'},unknown_code),
                                   ('absent',{'TEST_API':'404','TEST_INDEX':'404'},0)]:
                with self.subTest(path=path,state=label):
                    result=subprocess.run(['bash','-c',command],cwd=self.root,
                        env={**self.env,**env},capture_output=True,text=True,timeout=15)
                    self.assertEqual(result.returncode,code,result.stdout+result.stderr)
                    if label == 'unknown':
                        self.assertIn('inconclusive',result.stderr.lower())
                        self.assertIn('may already exist',result.stderr.lower())
                        self.assertNotIn('first cut still needed',result.stdout)

    def test_handoff_and_finish_verify_manifest_version(self):
        for path in ['owner-post-installable-handoff.sh','owner-finish-installable.sh',
                     'ci-branch-lite.sh','check-cut-path.sh']:
            text=(ROOT/'scripts'/path).read_text()
            matching=[line.strip() for line in text.splitlines()
                      if 'MODE=registry ' in line and 'bash scripts/verify-crates-io-consumer.sh' in line]
            self.assertEqual(len(matching),1,path)
            command='set -euo pipefail\nver=0.2.0\n'+matching[0]
            result=subprocess.run(['bash','-c',command],cwd=self.root,
                env={**self.env,'MODE':'path','VER':'0.1.0'},capture_output=True,text=True,timeout=15)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            calls=[json.loads(s) for s in self.calls.read_text().splitlines()]
            self.assertEqual([x['registry_version'] for x in calls if x['tool']=='cargo'][-1], '0.2.0')

    def test_audit_json_requires_registry_identity_and_external_consumer(self):
        text=(ROOT/'scripts/audit-civilization-bars.sh').read_text()
        start=text.index('if [[ "$JSON" == "1" ]]; then')
        end=text.index('\nfi',start)+3
        fragment=text[start:end]
        for identity,consumer,expected in [('present',0,'unknown'),('present',1,'present'),
                                            ('absent',0,'absent'),('unknown',1,'unknown')]:
            with self.subTest(identity=identity,consumer=consumer):
                command=('set -euo pipefail\nJSON=1\npass=0\npartial=0\nblocked=0\nfail=0\n'
                         'installable_blocked=0\nver=0.2.0\nPL_CRATES_IDENTITY_STATUS='+identity+'\n'
                         'registry_consumer_passed='+str(consumer)+'\n'+fragment)
                result=subprocess.run(['bash','-c',command],cwd=self.root,env=self.env,
                                      capture_output=True,text=True,timeout=5)
                self.assertEqual(result.returncode,0,result.stderr)
                self.assertEqual(json.loads(result.stdout)['installable'],expected)

    def run_status_fragment(self, name):
        path=ROOT/'scripts'/name
        text=path.read_text()
        start=text.index('# shellcheck source=scripts/lib/crates-io.sh')
        if name == 'audit-civilization-bars.sh': end=text.index('# Probe auth',start)
        else: end=text.index('\necho\necho "== Verifiable',start)
        fragment=text[start:end]
        prefix='set -euo pipefail\nROOT="$1"\nname=partitionline\nver=0.2.0\n'+\
               'ok(){ echo "PASS $*"; }; bad(){ echo "FAIL $*"; }; iblk(){ echo "BLOCKED $*"; }\n'
        result=subprocess.run(['bash','-c',prefix+fragment,'--',str(self.root)],cwd=self.root,
                              env={**self.env,'TEST_API':'503'},capture_output=True,text=True,timeout=15)
        return result

    def test_audit_and_status_do_not_call_index_only_confirmed(self):
        for path in ['audit-civilization-bars.sh','owner-status.sh']:
            with self.subTest(path=path):
                result=self.run_status_fragment(path)
                self.assertEqual(result.returncode,0,result.stderr)
                self.assertNotIn('PASS crates.io has',result.stdout)
                self.assertNotIn('OK  crates.io has',result.stdout)
                self.assertIn('inconclusive',result.stdout.lower())


if __name__ == '__main__':
    unittest.main()

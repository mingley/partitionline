#!/usr/bin/env python3
"""Execute checked-in release shell steps with isolated, offline service substitutes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = {
    'final-tag': 0, 'wrong-tag': 1, 'prerelease-tag': 1,
    'workflow-existing-version': 0, 'workflow-absent-version': 0,
    'workflow-registry-unavailable': 1,
    'local-existing-version': 0, 'local-registry-unavailable': 1,
    'workflow-index-pending': 1, 'local-index-pending': 1,
    'missing-exact-ci': 1,
    'confirmation-interrupted': 130, 'confirmation-resumed': 0,
    'release-notes-first': 0, 'release-notes-repeated': 0,
}
SOURCE_FILES = ['Cargo.toml', '.github/workflows/release.yml', '.github/workflows/release-plz.yml',
                'scripts/owner-publish.sh', 'scripts/lib/crates-io.sh',
                'scripts/lib/cargo-registry-token.sh', 'scripts/check-main-ci.sh']
SOURCE_FILES += ['scripts/report-release-rehearsal.py', 'scripts/rehearse-partial-release.sh']

# These substitutes have no path to their real executable or any network client.
# Unexpected commands fail, and every attempted command is retained.
SHIM = r'''import json, os
from pathlib import Path
import sys
name = Path(sys.argv[0]).name
args = sys.argv[1:]
folder = Path(os.environ['PL_REHEARSAL_CASE'])
with (folder/'commands.jsonl').open('a') as output:
    output.write(json.dumps({'command':name,'args':args})+'\n')
def deny():
    print('rehearsal refused external/mutating command: '+name, file=sys.stderr)
    raise SystemExit(97)
if name == 'git':
    if args == ['rev-parse','--abbrev-ref','HEAD']: print('main')
    elif args[:1] == ['status']: pass
    elif args[:1] == ['rev-parse']: print(os.environ['PL_REHEARSAL_SOURCE'])
    elif args[:2] == ['tag','-l']: print('fixture release note body')
    else: deny()
elif name == 'curl':
    if any(a in args for a in ['-X','--request','-T','--upload-file','--data','-d','--form','-F']): deny()
    url = args[-1]
    if not url.startswith(('https://crates.io/api/v1/crates/partitionline/','https://index.crates.io/pa/rt/partitionline')): deny()
    mode = os.environ['PL_REHEARSAL_REGISTRY']
    code = {'present':'200','absent':'404','unavailable':'503','api-only':'200'}[mode]
    if mode == 'api-only' and url.startswith('https://index.crates.io/'): code = '404'
    body = json.dumps({'vers':'0.1.0','yanked':False})+'\n' if mode=='present' else '{}\n'
    if '-o' in args: Path(args[args.index('-o')+1]).write_text(body)
    elif '--output' in args: Path(args[args.index('--output')+1]).write_text(body)
    else: print(body, end='')
    if '-w' in args: print(code, end='')
    if code != '200' and any(a.startswith('-') and 'f' in a and not a.startswith('--') for a in args): raise SystemExit(22)
elif name == 'sleep':
    # Model a SIGINT-equivalent stop at the first retry boundary; never wait.
    raise SystemExit(130)
elif name == 'gh':
    if args[:2] == ['release','view']:
        raise SystemExit(0 if (folder/'release-created.json').is_file() else 1)
    if args[:2] == ['release','create']:
        marker = folder/'release-created.json'
        if marker.exists(): deny()
        marker.write_text(json.dumps({'tag':args[2], 'simulated':True}))
    else: deny()
else: deny()
'''


def require(condition, message):
    if not condition:
        raise ValueError(message)


def workflow_steps(text):
    """Extract literal Bash run blocks; reject expressions not explicitly modeled."""
    result = {}
    for match in re.finditer(r'(?m)^      - name: ([^\n]+)\n((?:        .*\n|\n)*)', text):
        block = match.group(2)
        run = re.search(r'(?m)^        run: \|\n((?:          .*\n|\n)*)', block)
        if run:
            result[match.group(1)] = ''.join(line[10:] if line.startswith('          ') else line
                                           for line in run.group(1).splitlines(keepends=True))
    return result


def isolated_cases(report, source_root=ROOT, source_sha='1'*40):
    report.mkdir(parents=True, exist_ok=True)
    source_hashes = {name: hashlib.sha256((source_root/name).read_bytes()).hexdigest() for name in SOURCE_FILES}
    steps = workflow_steps((source_root/'.github/workflows/release.yml').read_text())
    results = []

    def case(name, step=None, registry='present', env_values=None, reuse=None, assertion=None):
        folder = report/'cases'/(reuse or name)
        if not folder.exists():
            folder.mkdir(parents=True)
            snapshot = folder/'source'
            (snapshot/'.github/workflows').mkdir(parents=True)
            for relative in SOURCE_FILES:
                path = snapshot/relative
                path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source_root/relative, path)
            (folder/'tmp').mkdir()
            (folder/'bin').mkdir()
            (folder/'ci-runs.json').write_text('[]\n')
            for command in ['git','curl','gh','cargo','sleep','rustup','wget','docker','ssh']:
                path = folder/'bin'/command
                path.write_text('#!'+sys.executable+'\n'+SHIM)
                path.chmod(0o755)
        env = {'PATH':str(folder/'bin')+':/usr/bin:/bin', 'LANG':'C.UTF-8',
               'TMPDIR':str(folder/'tmp'), 'PYTHONDONTWRITEBYTECODE':'1',
               'PL_REHEARSAL_CASE':str(folder), 'PL_REHEARSAL_SOURCE':source_sha,
               'PL_REHEARSAL_REGISTRY':registry, 'GITHUB_OUTPUT':str(folder/(name+'-outputs')),
               'EVENT_NAME':'push', 'REF_NAME':'v0.1.0', 'DISPATCH_TAG':'',
               'GH_RUNS_JSON':str(folder/'ci-runs.json'), 'REQUIRE_MAIN_CI':'1', 'REQUIRE_PROFILE_EVIDENCE':'1'}
        env.update(env_values or {})
        if step is None:
            args = ['bash','scripts/owner-publish.sh']
        else:
            text = steps[step].replace('${{ steps.already.outputs.skip }}', '0')
            require('${{' not in text, 'unmodeled workflow expression in '+step)
            text = text.replace('/tmp/pl-crate-probe.json',str(folder/'tmp/crate-probe.json'))
            text = text.replace('/tmp/pl-crate.json',str(folder/'tmp/crate.json'))
            (folder/(name+'.sh')).write_text(text)
            args = ['bash','-euo','pipefail',str(folder/(name+'.sh'))]
        try:
            process = subprocess.run(args, cwd=folder/'source', env=env,
                                     capture_output=True, text=True, timeout=15, check=False)
            code, stdout, stderr = process.returncode, process.stdout, process.stderr
        except subprocess.TimeoutExpired as error:
            code, stdout, stderr = 124, str(error.stdout or ''), str(error.stderr or '')
        (folder/(name+'.stdout.log')).write_text(stdout)
        (folder/(name+'.stderr.log')).write_text(stderr)
        passed = code == EXPECTED[name]
        if assertion is not None:
            passed = passed and assertion(folder, stdout, stderr)
        results.append({'name':name, 'status':'passed' if passed else 'failed',
                        'exit_code':code, 'expected_exit_code':EXPECTED[name],
                        'stdout':stdout, 'stderr':stderr, 'artifact_directory':str(folder),
                        'scope':'executed checked-in code with offline substituted services'})

    tag_step = 'Verify tag matches Cargo.toml (final versions only)'
    case('final-tag', tag_step)
    case('wrong-tag', tag_step, env_values={'REF_NAME':'v0.1.1'})
    case('prerelease-tag', tag_step, env_values={'REF_NAME':'v0.1.0-rc.1'})
    skip_step = 'Soft-skip if version already on crates.io'
    case('workflow-existing-version', skip_step, assertion=lambda f,o,e:
         (f/'workflow-existing-version-outputs').read_text()=='skip=1\n')
    case('workflow-absent-version', skip_step, registry='absent', assertion=lambda f,o,e:
         (f/'workflow-absent-version-outputs').read_text()=='skip=0\n')
    case('workflow-registry-unavailable', skip_step, registry='unavailable', assertion=lambda f,o,e:
         not (f/'workflow-registry-unavailable-outputs').exists() and 'registry unavailable' in e.lower())
    case('workflow-index-pending', skip_step, registry='api-only', assertion=lambda f,o,e:
         not (f/'workflow-index-pending-outputs').exists() and 'index=absent' in e)
    owner_env = {'CARGO_REGISTRY_TOKEN':'rehearsal-placeholder-not-a-credential','RUN_DAY1_AFTER_PUBLISH':'0'}
    case('local-existing-version', env_values=owner_env,
         assertion=lambda f,o,e:'skipping cargo publish' in o)
    case('local-registry-unavailable', registry='unavailable', env_values=owner_env,
         assertion=lambda f,o,e:'registry unavailable' in e.lower())
    case('local-index-pending', registry='api-only', env_values=owner_env,
         assertion=lambda f,o,e:'index=absent' in e)
    case('missing-exact-ci', 'Exact-SHA CI green (KL-08)',
         assertion=lambda f,o,e:'[outcome=missing]' in e)
    case('confirmation-interrupted', 'Confirm crates.io', registry='absent',
         assertion=lambda f,o,e:'waiting for crates.io index' in o)
    case('confirmation-resumed', 'Confirm crates.io', reuse='confirmation-interrupted',
         assertion=lambda f,o,e:'crates.io has partitionline 0.1.0' in o)
    case('release-notes-first', 'GitHub Release notes',
         assertion=lambda f,o,e:'created GitHub release v0.1.0' in o)
    case('release-notes-repeated', 'GitHub Release notes', reuse='release-notes-first',
         assertion=lambda f,o,e:'already exists' in o)
    commands = []
    for path in sorted((report/'cases').glob('*/commands.jsonl')):
        commands += [json.loads(line) for line in path.read_text().splitlines()]
    result = {'schema_version':1,'scope':'No-publish release recovery rehearsal; substituted services are not release or hosted CI evidence.',
              'candidate_source_sha':source_sha,'source_file_sha256':source_hashes,
              'scenarios':results,'commands':commands,'release_complete':False,
              'owner_actions':[{'stage':name,'status':'not_run','reason':reason} for name,reason in [
                  ('candidate-CI-and-package','Exact candidate hosted profile and full packed-crate checks remain required on an actual release.'),
                  ('release-authorization','Owner must select the next version and authorize the release.'),
                  ('authenticate-and-publish','No credential lookup, OIDC request or package upload occurs in rehearsal.'),
                  ('tag-and-push','No actual tag is created, moved or pushed.'),
                  ('registry-confirmation','Only local service responses were exercised; real registry confirmation follows actual publication.'),
                  ('GitHub-release-notes','Only a local marker was created; real note creation remains an owner release action.')]]}
    result['validation_success'] = all(row['status']=='passed' for row in results)
    (report/'scenario-report.json').write_text(json.dumps(result,indent=2)+'\n')
    validate(result)
    return result


def validate(result):
    require(result['schema_version']==1 and result['release_complete'] is False,
            'rehearsal cannot claim a completed release')
    require(re.fullmatch(r'[0-9a-f]{40}',result['candidate_source_sha']) is not None,'invalid exact source')
    rows=result['scenarios']
    require(len(rows)==len(EXPECTED) and {row['name'] for row in rows}==set(EXPECTED),
            'missing/duplicate/unknown rehearsal scenario')
    for row in rows:
        require(type(row['exit_code']) is int and type(row['expected_exit_code']) is int
                and row['exit_code']==row['expected_exit_code']==EXPECTED[row['name']]
                and row['status']=='passed', 'failed rehearsal scenario: '+row['name'])
    require(result['validation_success'] is True,'incomplete rehearsal')
    require(set(result['source_file_sha256'])==set(SOURCE_FILES)
            and all(re.fullmatch(r'[0-9a-f]{64}',value) for value in result['source_file_sha256'].values()),
            'missing source identity')
    forbidden=[]
    for command in result['commands']:
        name,args=command['command'],command['args']
        if name=='git' and args[:2] not in [['rev-parse','HEAD'],['rev-parse','--abbrev-ref'],['rev-parse','-q'],['tag','-l']] and args[:1]!=['status']:
            forbidden.append(command)
        if name in ['cargo','rustup','wget','docker','ssh'] or (name=='gh' and args[:2] not in [['release','view'],['release','create']]):
            forbidden.append(command)
    require(not forbidden,'forbidden command reached during rehearsal')
    creates=[c for c in result['commands'] if c['command']=='gh' and c['args'][:2]==['release','create']]
    require(len(creates)==1,'release notes must be simulated once across repeated steps')
    require(len(result['owner_actions'])==6 and all(row['status']=='not_run' for row in result['owner_actions']),
            'owner release actions cannot be claimed by a rehearsal')


def git(*args):
    return subprocess.check_output(['git',*args],cwd=ROOT,text=True).strip()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--self-test',action='store_true')
    group.add_argument('--run',type=Path,metavar='ARTIFACT_BASE')
    args=parser.parse_args()
    try:
        if args.self_test:
            with tempfile.TemporaryDirectory(prefix='pl-release-rehearsal-') as temporary:
                isolated_cases(Path(temporary))
        else:
            require(not git('status','--porcelain','--untracked-files=normal'),'clean committed source required')
            source=git('rev-parse','HEAD')
            tags=git('for-each-ref','--format=%(refname) %(objectname)','refs/tags')
            report=args.run.resolve()/(source+'-'+uuid.uuid4().hex)
            result=isolated_cases(report,source_sha=source)
            structural=subprocess.run(['bash','scripts/rehearse-partial-release.sh','--self-test'],
                cwd=ROOT,capture_output=True,text=True,timeout=60,check=False)
            (report/'structural-self-test.stdout.log').write_text(structural.stdout)
            (report/'structural-self-test.stderr.log').write_text(structural.stderr)
            require(structural.returncode==0,'structural/self-test release gate failed')
            result['structural_self_test']={'exit_code':structural.returncode,
                'stdout':structural.stdout,'stderr':structural.stderr}
            require(git('rev-parse','HEAD')==source and not git('status','--porcelain','--untracked-files=normal'),
                    'candidate source changed during rehearsal')
            require(git('for-each-ref','--format=%(refname) %(objectname)','refs/tags')==tags,'tags changed during rehearsal')
            result['completed_utc']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
            result['actual_tag_refs_unchanged']=True
            result['python_version']=sys.version
            (report/'report.json').write_text(json.dumps(result,indent=2)+'\n')
            print('Exact-source artifact: '+str(report))
        print('Release rehearsal: all 15 executed recovery scenarios passed; release remains an owner action')
    except (ValueError,KeyError,OSError) as error:
        print('Release rehearsal rejected: '+str(error),file=sys.stderr)
        return 1
    return 0


if __name__=='__main__':
    raise SystemExit(main())

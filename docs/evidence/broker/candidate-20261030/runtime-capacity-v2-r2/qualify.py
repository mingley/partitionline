#!/usr/bin/env python3
"""Qualify one pinned broker capacity change using the existing broker runner."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys

SPEC_SHA = 'a467ffefd723efcc32a8b810af41fa7a91f3ce83b4dfdd5485c11b5102df3749'
BASE = '6fa8cdb3588d884580d01331e44df657c02f7670'
NEGATIVE = 'raft::runtime::ownership_tests::proposal_capacity_is_checked_before_admission_and_wal_work'
POSITIVE = {
    'raft::runtime::ownership_tests::baseline_over_capacity_hits_admission_deadline_with_permits_held',
    'raft::runtime::ownership_tests::proposal_capacity_check_keeps_the_original_allocation',
}
FIXED = {
    'raft::runtime::ownership_tests::proposal_capacity_check_keeps_the_original_allocation',
    NEGATIVE,
}
STALE = 'stale_genesis_leader_observation_is_fenced_before_proposal_append'


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def expected_negative(text, line):
    """Require the expected assertion site within the exact test's failure body."""
    headings = list(re.finditer(r'^---- (.*?) stdout ----\s*$', text, re.M))
    matching = [m for m in headings if m.group(1) == NEGATIVE]
    if len(matching) != 1:
        return False
    start = matching[0].end()
    later = [m.start() for m in headings if m.start() > start]
    failure_summary = re.search(r'^failures:\s*$', text[start:], re.M)
    if failure_summary:
        later.append(start + failure_summary.start())
    section = text[start:min(later) if later else len(text)]
    panic = re.compile(r"^thread '" + re.escape(NEGATIVE) +
        r"'(?: \(\d+\))? panicked at [^\n]*src/raft/runtime\.rs:" + str(line) + r":\d+:\s*$", re.M)
    sites = list(panic.finditer(section))
    if len(sites) != 1 or len(re.findall(r'^thread .* panicked at ', section, re.M)) != 1:
        return False
    assertion = section[sites[0].end():].split('\nnote:', 1)[0].strip()
    normalized = re.sub(r'\s+', '', assertion)
    expected = ('assertionfailed:matches!('
                'tokio::time::timeout(Duration::from_secs(2),'
                'handle.propose(over_capacity)).await?,Err(Error::InvalidConfig))')
    return normalized == expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--checkout', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    checkout, output = args.checkout.resolve(), args.output.resolve()
    require(not output.is_relative_to(checkout) and not output.exists(), 'output must be new and outside checkout')
    output.mkdir(parents=True)
    report = {'passed': False, 'scope': 'Source-bound broker runtime capacity qualification; no production readiness or performance claim.',
              'base_commit': BASE, 'commands': [], 'gate_errors': []}
    runner = None
    try:
        require(sys.platform == 'linux' and platform.machine() == 'x86_64', 'Linux x86_64 required')
        actual = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=checkout, check=True,
                                capture_output=True, text=True, timeout=60).stdout.strip()
        require(actual == os.environ.get('GITHUB_SHA'), 'checkout does not equal triggering SHA')
        spec_path = Path(__file__).resolve().parent/'spec.json'
        require(spec_path.is_relative_to(checkout) and sha(spec_path) == SPEC_SHA, 'unexpected specification')
        spec = json.loads(spec_path.read_text())
        require(spec['base_commit'] == BASE and spec['kind'] == 'broker-runtime-capacity', 'unexpected source contract')
        require(set(spec['baseline_positive']) == POSITIVE and set(spec['baseline_negative']) == {NEGATIVE}
                and set(spec['fixed_cases']) == FIXED, 'test identities changed')
        for item in ('legacy_driver', 'ownership_helper'):
            path = checkout/spec[item]['path']
            require(not path.is_symlink() and path.is_file() and sha(path) == spec[item]['sha256'],
                    'qualification helper mismatch: '+item)
        driver_path = checkout/spec['legacy_driver']['path']
        module_spec = importlib.util.spec_from_file_location('broker_candidate_runner', driver_path)
        legacy = importlib.util.module_from_spec(module_spec);module_spec.loader.exec_module(legacy)
        def asset(key):
            item = spec[key];path = spec_path.parent/item['path']
            require(not path.is_symlink() and path.is_file() and path.stat().st_size == item['bytes']
                    and sha(path) == item['sha256'], 'asset mismatch: '+key)
            return path
        assets = {key:asset(key) for key in ('base_source','base_files','baseline_patch','fixed_patch',
                    'candidate_manifest','source_review','second_review','candidate_validation')}
        base = legacy.load(assets['base_files']); runtime = spec['runtime_path']
        require(base[runtime] == spec['base_runtime'] and
                base['partitionline-broker/Cargo.lock']['sha256'] == spec['broker_lock_sha256'],
                'runtime or broker lockfile differs from base catalog')
        baseline = dict(base); baseline[runtime] = spec['baseline_runtime']
        fixed = dict(base); fixed[runtime] = spec['fixed_runtime']
        source = output/'source';source.mkdir()
        legacy.extract_source(assets['base_source'], source, base);legacy.verify_source(source,base)
        saved = output/'assets';saved.mkdir()
        for path in [spec_path, Path(__file__), driver_path, checkout/spec['ownership_helper']['path'], *assets.values()]:
            shutil.copyfile(path, saved/path.name)
        report.update(trigger_sha=actual, specification_sha256=SPEC_SHA,
            wrapper_sha256=sha(Path(__file__)), base_source_sha256=sha(assets['base_source']),
            base_file_count=len(base), broker_lock_sha256=spec['broker_lock_sha256'],
            legacy_driver_sha256=sha(driver_path), ownership_helper_sha256=sha(checkout/spec['ownership_helper']['path']),
            baseline_expectations='negative and Deadline witness predicted; no baseline result until executed')
        def apply(patch, reverse=False):
            flags = ['--reverse'] if reverse else []
            for check in (True, False):
                subprocess.run(['git','apply',*flags,*(['--check'] if check else []),str(patch)],
                               cwd=source,check=True,capture_output=True,timeout=120)
        apply(assets['baseline_patch'])
        legacy.restore_patched_source_modes(source, baseline, {runtime}, output/'baseline-source-modes.json')
        runner = legacy.Qualification(output, source, baseline)
        runner.run('rustc-version',['rustc','+stable','-Vv'])
        runner.run('cargo-version',['cargo','+stable','-V'])
        runner.run('openssl-prerequisite',['openssl','version'])
        old = runner.build('baseline')[0]
        runner.exact('baseline-positive-witnesses',old,POSITIVE)
        runner.exact('baseline-expected-negative',old,{NEGATIVE},negative=True)
        negative_log = (output/'baseline-expected-negative.log').read_text(errors='replace')
        require(expected_negative(negative_log,spec['baseline_negative_panic_line']),
                'baseline failed at a different site or did not expose the expected assertion')
        report['baseline_negative_expected_assertion'] = True
        apply(assets['baseline_patch'],reverse=True)
        legacy.restore_patched_source_modes(source,base,{runtime},output/'base-restored-modes.json')
        apply(assets['fixed_patch'])
        legacy.restore_patched_source_modes(source,fixed,{runtime},output/'fixed-source-modes.json')
        runner.expected = fixed
        for path in (source/'partitionline-broker/src').rglob('*.rs'):
            path.touch()
        gate_errors = report['gate_errors']
        def attempt(label, operation):
            try:
                return operation()
            except legacy.CommandFailure as error:
                row = {'gate': label, 'error': str(error)}
                if runner.records:
                    last = runner.records[-1]
                    row['test_failures'] = last.get('test_failures', [])[:50]
                    row['test_result_counts'] = last.get('test_result_counts', [])[:50]
                gate_errors.append(row)
                print(json.dumps({'gate_failure':row},ensure_ascii=True)[:8192],flush=True)
                return None
        for profile, all_features in [('default',False),('all-features',True)]:
            unit = attempt('fixed-'+profile+'-build',lambda:runner.build('fixed-'+profile,all_features))
            if unit:
                attempt('fixed-'+profile+'-capacity',lambda:runner.exact('fixed-'+profile+'-capacity',unit[0],FIXED))
                attempt('fixed-'+profile+'-legacy-focused',lambda:runner.exact('fixed-'+profile+'-legacy-focused',unit[0],legacy.FIXED))
                attempt('fixed-'+profile+'-async-filesystem',lambda:runner.exact('fixed-'+profile+'-async-filesystem',unit[0],legacy.ASYNC_FS_CASES))
            label = 'fixed-'+profile+'-all-targets'
            attempt(label,lambda:runner.full(label,all_features))
            artifacts = runner.built_artifacts.get(label,[])
            fetch = [a for a in artifacts if a['cargo_artifact']['target']['name']=='fetch']
            if len(fetch)!=1:
                gate_errors.append({'gate':'fixed-'+profile+'-fetch-repeats','error':'missing unique Fetch test executable'})
            else:
                for iteration in range(legacy.FETCH_DEADLINE_REPEATS):
                    name='fixed-'+profile+'-fetch-deadline-repeat-'+str(iteration+1)
                    attempt(name,lambda:runner.exact(name,fetch[0],{legacy.FETCH_DEADLINE_CASE}))
            if profile=='default':
                membership=[a for a in artifacts if a['cargo_artifact']['target']['name']=='raft_membership']
                if len(membership)!=1:
                    gate_errors.append({'gate':'stale-leader-exact','error':'missing unique membership test executable'})
                else:
                    attempt('stale-leader-exact',lambda:runner.exact('stale-leader-exact',membership[0],{STALE}))
        attempt('source-format',lambda:runner.run('source-format',['cargo','+stable','fmt','--manifest-path','partitionline-broker/Cargo.toml','--check']))
        attempt('strict-clippy',lambda:runner.run('strict-clippy',['cargo','+stable','clippy','--locked','--manifest-path','partitionline-broker/Cargo.toml','--all-targets','--all-features','--','-D','warnings'],1200))
        runner.env['RUSTDOCFLAGS']='-D warnings'
        attempt('strict-docs',lambda:runner.run('strict-docs',['cargo','+stable','doc','--locked','--manifest-path','partitionline-broker/Cargo.toml','--all-features','--no-deps'],1200))
        for profile,flags in [('default',[]),('all-features',['--all-features'])]:
            name='strict-doctests-'+profile
            attempt(name,lambda:runner.run(name,['cargo','+stable','test','--locked','--manifest-path','partitionline-broker/Cargo.toml','--doc',*flags],1200))
        attempt('broker-inventory',lambda:runner.run('broker-inventory',['python3','-B','scripts/check-broker-api-matrix.py','--report',str(output/'broker-inventory-report.json')]))
        controls=attempt('broker-inventory-negative-controls',lambda:runner.run('broker-inventory-negative-controls',
            ['python3','-B','-m','unittest','discover','-s','tests/ci','-p','test_broker_api_matrix.py','-v'],900))
        if controls is not None:
            require(re.search(r'^Ran 55 tests in ',controls,re.M) and re.search(r'^OK$',controls,re.M), 'inventory control suite incomplete')
        legacy.verify_source(source,fixed)
        report['passed'] = not gate_errors
    except Exception as error:
        report['error'] = str(error)
    finally:
        if runner is not None:
            report['commands'] = runner.records
        (output/'qualification.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'error':report.get('error'),
        'gate_errors':report['gate_errors'],'output':str(output)},ensure_ascii=True)[:16384],flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

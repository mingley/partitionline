#!/usr/bin/env python3
"""Compile predeclared invariant mutants against real public-API durable scenarios."""
import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace_once(source, before, after):
    assert source.count(before) == 1, (before, source.count(before))
    return source.replace(before, after, 1)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--source-sha', required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--scratch', type=Path, required=True)
    p.add_argument('--target', type=Path, required=True)
    args = p.parse_args()
    original = args.source.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    args.scratch.mkdir(parents=True, exist_ok=False)
    probe = Path(__file__).parent / 'replication_guard_probe.rs'
    source_path = Path('partitionline-broker/src/raft/replication.rs')
    source = (original / source_path).read_text()
    original_sha = sha(original / source_path)
    variants = [
        ('baseline', None, None),
        ('exact-response-target', 'exact_sent_target_cannot_be_forged_inside_the_local_durable_tail',
         [('response.matched != outstanding.target\n                || response.conflict_index != 0', 'response.conflict_index != 0')]),
        ('distinct-majority', 'five_voters_cannot_commit_with_only_one_remote_durable_match',
         [('let index = matched[count - (count / 2 + 1)];', 'let index = matched[count - 2];')]),
        ('current-term-commit', 'a_new_leader_cannot_commit_an_old_term_partial_chunk_before_its_barrier_matches',
         [('if index > self.log.committed && self.log.term_at(index) == Some(state.persistent.term)', 'if index > self.log.committed'),
          ('leader != self.local || sequence != 0 || self.term_at(index) != Some(term)', 'leader != self.local || sequence != 0')]),
        ('follower-matched-end', 'a_follower_cannot_commit_beyond_this_requests_verified_prefix',
         [('.min(target.index)', '')]),
    ]
    # Freeze the experiment before any run, including intentionally paired guards
    # for current-term commit (owner policy plus defensive runtime WAL validation).
    plan = {'source_sha': args.source_sha, 'source_file': str(source_path),
            'baseline_source_sha256': original_sha, 'probe_sha256': sha(probe),
            'variants': [{'name': name, 'test': test, 'replacements': changes} for name, test, changes in variants],
            'expectation': 'baseline four cases pass; each predeclared mutant fails its named real-durability scenario; no production source is changed'}
    (output / 'plan.json').write_text(json.dumps(plan, indent=2) + '\n')
    results = []
    env = os.environ.copy()
    env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
               CARGO_INCREMENTAL='0', CARGO_PROFILE_TEST_DEBUG='0', CARGO_PROFILE_DEV_DEBUG='0',
               CARGO_TARGET_DIR=str(args.target.resolve()))
    env['PATH'] = '/workspace/work/cargo/bin:' + env.get('PATH', '')
    for name, test, changes in variants:
        root = args.scratch / name
        root.mkdir()
        shutil.copytree(original / 'partitionline-broker', root / 'partitionline-broker')
        shutil.copy(original / 'clippy.toml', root / 'clippy.toml')
        # Retain original static inputs used by any compiled include paths.
        destination = root / 'tests/conformance/broker'
        destination.parent.mkdir(parents=True)
        shutil.copytree(original / 'tests/conformance/broker', destination)
        shutil.copy(probe, root / 'partitionline-broker/tests/replication_guard_probe.rs')
        changed = source
        for before, after in changes or []:
            changed = replace_once(changed, before, after)
        (root / source_path).write_text(changed)
        case = output / name
        case.mkdir()
        (case / 'mutation.patch').write_text(''.join(difflib.unified_diff(source.splitlines(True), changed.splitlines(True), fromfile=str(source_path), tofile=str(source_path))))
        cargo = ['taskset', '-c', '0-2,4', 'cargo', '+stable', 'test', '--manifest-path',
                 'partitionline-broker/Cargo.toml', '--test', 'replication_guard_probe', '-j1']
        if test:
            cargo.append(test)
        cargo += ['--', '--nocapture']
        current_env = env.copy()
        current_env['PL_REPLICATION_GUARD_DIR'] = str(case / 'raw-journals')
        started = time.monotonic()
        with (case / 'test.log').open('wb') as log:
            result = subprocess.run(cargo, cwd=root, env=current_env, stdout=log, stderr=subprocess.STDOUT)
        text = (case / 'test.log').read_text()
        if name == 'baseline':
            valid = result.returncode == 0 and '4 passed; 0 failed' in text
        else:
            valid = result.returncode != 0 and f'{test} ... FAILED' in text and '1 failed' in text and 'could not compile' not in text
        row = {'name': name, 'argv': cargo, 'exit_code': result.returncode,
               'expected_outcome_verified': valid, 'elapsed_seconds': time.monotonic()-started,
               'source_sha256': sha(root / source_path), 'probe_sha256': sha(probe),
               'log_sha256': sha(case / 'test.log'), 'patch_sha256': sha(case / 'mutation.patch'),
               'raw_journals': [{'path':str(file.relative_to(output)), 'sha256':sha(file), 'bytes':file.stat().st_size}
                                for file in sorted((case / 'raw-journals').rglob('*')) if file.is_file()]}
        results.append(row)
        (output / 'results-partial.json').write_text(json.dumps(results, indent=2) + '\n')
        if not valid:
            raise SystemExit(f'unexpected guard outcome retained: {name}')
    assert sha(original / source_path) == original_sha
    (output / 'results.json').write_text(json.dumps({'schema_version':1, 'source_sha':args.source_sha,
        'baseline_four_probes_pass':True, 'four_predeclared_mutants_fail_named_tests':True,
        'immutable_source_unchanged':True, 'plan_sha256':sha(output/'plan.json'), 'results':results,
        'scope':'Actual compiled Node public-API/election/WAL scenarios on isolated minimal archives. Current-term mutant removes both owner policy and its defensive runtime WAL equality guard; replay guard remains, and no mutant is published.'},indent=2)+'\n')
    print(json.dumps([{'name':r['name'],'exit_code':r['exit_code'],'expected':r['expected_outcome_verified']} for r in results]))


if __name__ == '__main__':
    main()

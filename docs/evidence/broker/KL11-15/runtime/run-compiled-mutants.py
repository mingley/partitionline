#!/usr/bin/env python3
"""Build predeclared unsafe guards in isolated archives and retain real failures."""
import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--source-sha', required=True)
    p.add_argument('--scratch', type=Path, required=True)
    p.add_argument('--target', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--binaries', type=Path, required=True)
    args = p.parse_args()
    assert re.fullmatch('[0-9a-f]{40}', args.source_sha)
    evidence = Path(__file__).resolve().parent
    plan_path = evidence / 'compiled-mutant-plan.json'
    plan = json.loads(plan_path.read_text())
    relative = Path(plan['production_source'])
    assert digest(args.source / relative) == plan['production_sha256']
    original = (args.source / relative).read_text()
    inputs = {str(f.relative_to(args.source)): digest(f)
              for f in args.source.rglob('*') if f.is_file()}
    args.scratch.mkdir(parents=True, exist_ok=False)
    args.output.mkdir(parents=True, exist_ok=False)
    args.binaries.mkdir(parents=True, exist_ok=False)
    root = args.scratch / 'source'
    # Reuse immutable inputs without copying hundreds of MiB of prior evidence.
    # Break the sole edited hardlink before writing any variant.
    shutil.copytree(args.source, root, copy_function=os.link)
    (root / relative).unlink()
    commands = []
    env = os.environ.copy()
    env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
               CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
               CARGO_TARGET_DIR=str(args.target.resolve()))
    env['PATH'] = '/workspace/work/cargo/bin:' + env.get('PATH', '')
    for key in ['PL_SNAPSHOT_FAILURE_DIR', 'PL_SNAPSHOT_REOPEN_DIR',
                'PARTITIONLINE_METADATA_LIVE_PORT', 'PARTITIONLINE_PRODUCE_LIVE_PORT',
                'PARTITIONLINE_FETCH_LIVE_PORT']:
        env.pop(key, None)
    for mutant in plan['mutants']:
        for baseline in [True, False]:
            name = mutant['name'] + ('-positive' if baseline else '-unsafe')
            case = args.output / name
            case.mkdir()
            assert original.count(mutant['old']) == 1
            text = original if baseline else original.replace(mutant['old'], mutant['new'], 1)
            (root / relative).write_text(text)
            patch = ''.join(difflib.unified_diff(original.splitlines(True), text.splitlines(True),
                                               fromfile=str(relative), tofile=str(relative)))
            (case / 'mutation.patch').write_text(patch)
            local = env.copy()
            local['PL_SNAPSHOT_RESPONSE_DIR'] = str(case / 'raw')
            command = ['taskset', '-c', '0-2,4', 'cargo', '+stable', 'test',
                       '--manifest-path', 'partitionline-broker/Cargo.toml', '-j1',
                       '--test', 'raft_replication', mutant['test'], '--', '--exact', '--nocapture']
            log = case / 'test.log'
            with log.open('wb') as stream:
                result = subprocess.run(command, cwd=root, env=local,
                                        stdout=stream, stderr=subprocess.STDOUT)
            outcome = log.read_text()
            valid = ((result.returncode == 0 and '1 passed; 0 failed' in outcome) if baseline
                     else (result.returncode == 101 and '1 failed' in outcome
                           and mutant['test'] + ' ... FAILED' in outcome
                           and 'could not compile' not in outcome))
            binary = re.search(r'Running tests/raft_replication.rs \(([^)]+)\)', outcome)
            assert binary, 'no compiled runtime artifact: ' + name
            binary_path = Path(binary.group(1))
            if not binary_path.is_absolute():
                binary_path = root / binary_path
            saved = args.binaries / (name + '-' + digest(binary_path)[:16])
            shutil.copy2(binary_path, saved)
            row = {'name': name, 'source_sha': args.source_sha, 'argv': command,
                   'cwd': str(root), 'exit_code': result.returncode,
                   'expected_behavior_verified': valid, 'source_sha256': digest(root / relative),
                   'log': str(log.relative_to(args.output)), 'log_sha256': digest(log),
                   'patch_sha256': digest(case / 'mutation.patch'),
                   'binary_path': str(saved), 'binary_sha256': digest(saved),
                   'binary_bytes': saved.stat().st_size}
            commands.append(row)
            (args.output / 'results-partial.json').write_text(json.dumps(commands, indent=2) + '\n')
            assert valid, 'unexpected outcome retained: ' + name
    assert inputs == {str(f.relative_to(args.source)): digest(f)
                      for f in args.source.rglob('*') if f.is_file()}
    (root / relative).write_text(original)
    (args.output / 'validation.json').write_text(json.dumps({'schema_version': 1,
        'source_sha': args.source_sha, 'predeclared_plan_sha256': digest(plan_path),
        'three_positive_controls_pass': True, 'three_compiled_unsafe_guards_fail': True,
        'immutable_original_files_unchanged': True, 'commands': commands,
        'scope': 'Actual compiled Node/Journal tests; guard mutants are never published. '
                 'Compiler/lint failures do not count as behavioral counterexamples.'}, indent=2) + '\n')
    print(json.dumps([{'name':r['name'], 'exit_code':r['exit_code']} for r in commands]))


if __name__ == '__main__':
    main()

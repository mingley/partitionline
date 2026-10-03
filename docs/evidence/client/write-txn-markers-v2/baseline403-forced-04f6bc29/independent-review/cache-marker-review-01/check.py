#!/usr/bin/env python3
"""Independent source/memory controls. No marker, Cargo, Docker or cleanup."""
import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import stat

BASE = Path('/workspace/work/client-capability-qa-preparation-e90efb49')
OUT = Path('/workspace/work/integration/cache-marker-review-01')
V4 = BASE / 'run-forced-baseline403-standard-marker-v4.py'
PREP = BASE / 'cache-tag-protection-preparation-v4'


def metadata(path):
    st = path.lstat()
    assert stat.S_ISREG(st.st_mode) and st.st_size <= 2 * 1024 * 1024
    return {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'bytes': st.st_size,
            'full07777': stat.S_IMODE(st.st_mode), 'mtime_ns': st.st_mtime_ns}


def cleanup_block(raw):
    tree = ast.parse(raw)
    block = next(n for n in tree.body if isinstance(n, ast.Try))
    start = next(i for i, n in enumerate(block.body) if isinstance(n, ast.Assign) and
                 any(isinstance(t, ast.Name) and t.id == 'afterclean' for t in n.targets))
    end = next(i for i, n in enumerate(block.body[start:], start) if isinstance(n, ast.Expr) and
               isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name) and
               n.value.func.id == 'docker_zero_running_workloads')
    return ast.Module(body=block.body[start:end], type_ignores=[])


def trial(ns, runner, added=None):
    model = ns['Model']()
    tag = ns['TAG']
    target = ns['TARGET']
    model.put(target + '/CACHEDIR.TAG', tag)
    model.expected['CACHEDIR.TAG'] = {'sha256': hashlib.sha256(tag).hexdigest(),
                                    'bytes': 177, 'full_mode': 0o600, 'mtime_ns': 123}
    original = {p: dict(row) for p, row in model.expected.items()}
    assert len(original) == 609 and len(model.own) == 25
    for p in model.own:
        del model.data[target + '/' + p]
        del model.modes[target + '/' + p]
        del model.mtimes[target + '/' + p]
    if added:
        model.put(target + '/unreviewed-after-clean', b'new unreviewed regular file')
        if added == 'symlink':
            model.modes[target + '/unreviewed-after-clean'] = stat.S_IFLNK | 0o777
    environment = model.namespace()
    environment['checked_preclean'] = {'complete_cache_identity_map': original}
    environment['INITIAL_PACKAGE_IDENTITIES'] = model.own
    tree = ast.parse(runner.read_bytes())
    extra = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'verify_exact_afterclean_paths']
    if extra:
        exec(compile(ast.fix_missing_locations(ast.Module(body=extra, type_ignores=[])), str(runner) + '-strict-set-helper-only', 'exec'), environment)
    try:
        exec(compile(ast.fix_missing_locations(cleanup_block(runner.read_bytes())), str(runner) + '-extracted-cleanup-only', 'exec'), environment)
    except AssertionError:
        return {'accepted': False, 'actual_operations': 0, 'remaining_mock_paths': len(model.files(target))}
    return {'accepted': True, 'actual_operations': 0, 'remaining_mock_paths': len(model.files(target))}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--candidate')
    p.add_argument('--candidate-sha256')
    p.add_argument('--label', required=True)
    args = p.parse_args()
    assert tuple(sorted(os.sched_getaffinity(0))) == (2, 4)
    assert args.label.replace('-', '').isalnum()
    candidates = [V4, PREP / 'check-marker-dryrun-controls.py', PREP / 'marker-dryrun-controls.json',
        PREP / 'proposal.json', PREP / 'frozen-packet-manifest.json',
        BASE / 'cache-tag-protection-preparation/standard-CACHEDIR.TAG.proposed',
        BASE / 'platform-daemon-exception-preparation-v2/current-complete-cache-hash-fullmode-map.json',
        BASE / 'package-invalidation-proposal-after-baseline-setup/proposal.json',
        BASE / 'platform-daemon-exception-preparation/exact-platform-daemons.json']
    if args.candidate:
        candidates.append(Path(args.candidate))
    before = {str(path): metadata(path) for path in candidates}
    assert before[str(V4)]['sha256'] == '8065020485bec6865e09ffb567130194ccf9bdc6f3121fa5efc032234ea908fe'
    controls = json.loads(candidates[2].read_bytes())
    assert before[str(candidates[1])]['sha256'] == controls['control_source_sha256']
    control_source = candidates[1].read_bytes()
    tree = ast.parse(control_source)
    stop = next(i for i, n in enumerate(tree.body) if isinstance(n, ast.Assign) and
                any(isinstance(t, ast.Name) and t.id == 'receipt' for t in n.targets))
    prefix = ast.Module(body=tree.body[:stop], type_ignores=[])
    # The reviewed prefix reads pinned public source/tag definitions and runs
    # memory-only Model controls. Final real receipt writes are excluded.
    namespace = {'__file__': str(candidates[1]), '__name__': 'independent_memory_controls'}
    exec(compile(ast.fix_missing_locations(prefix), str(candidates[1]) + '-no-receipt-writes', 'exec'), namespace)
    assert len(namespace['RESULTS']) == 28 and all(row['passed'] for row in namespace['RESULTS'])
    reproduced = {'positive584': trial(namespace, V4),
                  'unreviewed_regular_counterexample': trial(namespace, V4, 'regular'),
                  'unreviewed_symlink_counterexample': trial(namespace, V4, 'symlink')}
    assert reproduced['positive584']['accepted'] and all(reproduced[k]['accepted'] for k in
        ['unreviewed_regular_counterexample', 'unreviewed_symlink_counterexample'])
    derivative = None
    if args.candidate:
        path = Path(args.candidate)
        assert before[str(path)]['sha256'] == args.candidate_sha256
        v4_funcs = {n.name: ast.dump(n, include_attributes=False) for n in ast.parse(V4.read_bytes()).body if isinstance(n, ast.FunctionDef)}
        v5_funcs = {n.name: ast.dump(n, include_attributes=False) for n in ast.parse(path.read_bytes()).body if isinstance(n, ast.FunctionDef)}
        assert all(v5_funcs[name] == value for name, value in v4_funcs.items())
        assert set(v5_funcs) - set(v4_funcs) == {'verify_exact_afterclean_paths'}
        derivative = {'source': before[str(path)], 'positive584': trial(namespace, path),
                      'reject_added_regular': trial(namespace, path, 'regular'),
                      'reject_added_symlink': trial(namespace, path, 'symlink'),
                      'all_existing_function_ASTs_byte_semantics_unchanged': sorted(v4_funcs)}
        assert derivative['positive584']['accepted'] and not derivative['reject_added_regular']['accepted'] and not derivative['reject_added_symlink']['accepted']
    after = {str(path): metadata(path) for path in candidates}
    assert before == after
    data = json.dumps({'schema_version': 1, 'scope': 'independent extracted source/memory controls only',
        'affinity': [2, 4], 'actual_tag_cache_source_writes': 0, 'actual_Cargo_Docker_SDK_cleanup_commands': 0,
        'input_before': before, 'input_after': after, 'all_inputs_bytes_full07777_mtimes_unchanged': True,
        'inherited28_source_controls_independently_passed': namespace['RESULTS'],
        'v4_concrete_pathset_counterexample': reproduced, 'candidate_strict_set_controls': derivative,
        'root_requested_scope': 'standard177B-only marker plus historical608 through609 dryrun and strict584 survivors; root alone authorizes operational launch',
        'limitations': ['No actual repair, dryrun, cleanup, compiler, Docker query or API behavior executed.',
                        'These are exact extracted post-clean source guards applied to a bounded memory model.']},
        sort_keys=True, indent=2).encode() + b'\n'
    output = OUT / (args.label + '.json')
    assert not output.exists() and len(data) <= 2 * 1024 * 1024
    with output.open('xb') as stream:
        stream.write(data)
    output.chmod(0o600)
    print(json.dumps({'path': str(output), 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data),
                      'actual_operations': 0, 'v4_counterexamples_reproduced': 2,
                      'candidate_controls_pass': derivative is not None}))


if __name__ == '__main__':
    main()

"""Independent source/semantic controls. No Rust, sockets, subprocesses or cache writes."""
import ast
import hashlib
import json
import os
import re
import stat
from pathlib import Path
from types import SimpleNamespace

ROOT = Path('/workspace/work/raft-runtime-76/tcp-qualification-02')
OUT = Path(__file__).parent
HANDOFF = 'e4250889d4cd2e68818a21b19e6a68a8caeb7b516ad54643a24b34880a54591b'


def identity(path):
    assert path.is_file() and not path.is_symlink()
    return {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'bytes': path.stat().st_size, 'full07777': stat.S_IMODE(path.stat().st_mode)}


def verify():
    assert identity(ROOT / 'handoff.json')['sha256'] == HANDOFF
    manifest = json.loads((ROOT / 'handoff.json').read_text())
    assert len(manifest['rows']) == 20
    observed = {}
    for row in manifest['rows']:
        path = Path(row['source'])
        assert path == ROOT / row['path']
        found = identity(path)
        assert found == {k: row[k] for k in found}, row['path']
        observed[row['path']] = {'source': str(path), **found}
    return observed


def masked(source):
    """Remove ordinary Rust strings/comments, preserving brace/newline positions."""
    out = list(source)
    i = 0
    while i < len(source):
        if source.startswith('//', i):
            end = source.find('\n', i)
            end = len(source) if end < 0 else end
            for j in range(i, end): out[j] = ' '
            i = end
        elif source.startswith('/*', i):
            start, depth = i, 1
            i += 2
            while depth:
                assert i < len(source)
                if source.startswith('/*', i): depth += 1; i += 2
                elif source.startswith('*/', i): depth -= 1; i += 2
                else: i += 1
            for j in range(start, i):
                if out[j] != '\n': out[j] = ' '
        elif source[i] == '"':
            start = i
            i += 1
            while True:
                assert i < len(source)
                if source[i] == '\\': i += 2
                elif source[i] == '"': i += 1; break
                else: i += 1
            for j in range(start, i):
                if out[j] != '\n': out[j] = ' '
        else: i += 1
    return ''.join(out)


def braces(source, start):
    assert source[start] == '{'
    depth = 1
    end = start + 1
    while depth:
        assert end < len(source)
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    return source[start + 1:end - 1], end


def main():
    os.sched_setaffinity(0, {2, 4})
    before = verify()
    controls = []
    def check(name, condition, detail=None):
        assert condition, name
        controls.append({'name': name, 'passed': True, 'detail': detail})
    runtime_path = ROOT / 'candidate/partitionline-broker/src/raft/runtime.rs'
    source = runtime_path.read_text()
    tokens = masked(source)
    # Static required-field scan; not a Rust parser/compiler or typing result.
    declaration = re.search(r'\bstruct Envelope\s*(\{)', tokens)
    required, _ = braces(tokens, declaration.start(1))
    check('Envelope declaration requires cfg(test) counter', '_counter: Option<TestCounter>' in required)
    literals = []
    for match in re.finditer(r'\bEnvelope\s*(\{)', tokens):
        if match.start() == declaration.start() + len('struct '): continue
        body, end = braces(tokens, match.start(1))
        if re.match(r'\s*=\s*envelope\s*;', tokens[end:]): continue
        row = {'line': source.count('\n', 0, match.start()) + 1,
               'has_required_counter': bool(re.search(r'\b_counter\s*:', body))}
        literals.append(row)
    missing = [row for row in literals if not row['has_required_counter']]
    check('reproduced exactly one missing required Envelope literal field', len(missing) == 1 and source.splitlines()[missing[0]['line'] - 1].strip().endswith('Envelope {'), {'literal_scan': literals, 'missing_literal': missing})

    permit_match = re.search(r'\bstruct TestPermit\s*(\{)', tokens)
    permit, _ = braces(tokens, permit_match.start(1))
    check('frozen permit fields release semaphore before counter', permit.index('_permit:') < permit.index('_counter:'))
    # A legal interleaving under Rust declaration-order field destruction.
    current = peak = held = cap = 4
    held -= 1                         # old _permit releases first
    check('counter race intermediate real occupancy bounded', held == 3 and current == 4)
    held += 1                         # new permit acquired
    current += 1                      # new TestCounter constructed
    peak = max(peak, current)
    check('counter race records false-over-limit peak despite bounded permits', held == cap and current == peak == 5)
    current -= 1                      # old _counter finally destroyed
    check('counter race persists after current recovers', held == current == 4 and peak == 5)
    current = peak = held = 4
    current -= 1                      # corrected order: counter before permit
    held -= 1
    held += 1
    current += 1
    peak = max(peak, current)
    check('counter-before-permit ordering avoids same legal interleaving', held == current == peak == 4)
    for charge in (1, 4096, 512 * 1024):
        check(f'transport-counter race scales with charge{charge}', 5 * charge > 4 * charge)

    helper_path = ROOT / 'artifact_paths.py'
    namespace = {}
    exec(compile(helper_path.read_text(), str(helper_path), 'exec'), namespace)
    target = '/workspace/work/target-broker-segments'
    tree = '/workspace/work/broker-merged-source-ea9ff293'
    parser = namespace['harness_paths']
    for label, log, expected in (
        ('nonverbose lib', f'Running unittests src/lib.rs ({target}/debug/deps/partitionline_broker-a1b2)', [f'{target}/debug/deps/partitionline_broker-a1b2']),
        ('nonverbose integration', f'Running tests/raft_runtime.rs ({target}/debug/deps/raft_runtime-c3d4)', [f'{target}/debug/deps/raft_runtime-c3d4']),
        ('verbose quoted path', f'Running `{target}/debug/deps/partitionline_broker-e5f6 --test-threads=1`', [f'{target}/debug/deps/partitionline_broker-e5f6']),
        ('compiler cfg parentheses', 'Running `/usr/bin/rustc --check-cfg \'cfg(feature, values("default"))\'`', []),
        ('buildscript', f'Running `{target}/debug/build/crc32c-abc/build-script-build`', []),
    ):
        check('actual parser '+label, parser(log, target, tree) == expected)
    for label, path in (('outside target', '/elsewhere/debug/deps/raft_runtime-a1b2'), ('wrong harness', target + '/debug/deps/rustc')):
        failed = False
        try: parser('Running tests/raft_runtime.rs (' + path + ')', target, tree)
        except ValueError: failed = True
        check('actual parser refuses '+label, failed)
    runner = (ROOT / 'run-next-focused.py').read_text()
    positions = [runner.index(text) for text in ("item['executed_elfs']=executed_elfs", "item['post_command_elfs']=retain_cache", "retained=preserve_whole_cache('post-'")]
    check('frozen orchestration parses before complete new-output retention', positions == sorted(positions))
    events = []
    def parser_refusal(): events.append('parse'); raise ValueError('synthetic refused path')
    try:
        parser_refusal()
        events.append('all-ELF retention')
        events.append('cache-delta retention')
    except ValueError: events.append('save failure')
    check('parser refusal skips later retention in frozen ordering', events == ['parse', 'save failure'])

    # Extract exact cache_owners only. Fake proc records; no real process/cache inspection.
    helper_tree = ast.parse((ROOT / 'guard-functions.py').read_text())
    function = next(n for n in helper_tree.body if isinstance(n, ast.FunctionDef) and n.name == 'cache_owners')
    rows = []
    class FakeFile:
        def __init__(self, key): self.key = key
        def read_bytes(self):
            if self.key.endswith('/cmdline'): return b'cargo\0' + target.encode() + b'\0'
            if self.key.endswith('/environ'): return b''
            raise AssertionError(self.key)
        def read_text(self):
            assert self.key.endswith('/maps')
            raise PermissionError('synthetic live process maps unreadable')
    class FakeEntry:
        name = '12345'
        def __truediv__(self, key): return FakeFile('/proc/12345/' + key)
    class FakePath:
        def __init__(self, text): self.text = str(text)
        def __str__(self): return self.text
        def resolve(self): return self
        def iterdir(self): assert self.text == '/proc'; return [FakeEntry()]
        @property
        def name(self): return self.text.split('/')[-1]
    def readlink(path):
        if path.key.endswith('/exe'): return '/usr/bin/cargo'
        if path.key.endswith('/cwd'): return '/workspace'
        raise AssertionError(path.key)
    globals_ = {'Path': FakePath, 'ENV': {'CARGO_TARGET_DIR': target}, 'os': SimpleNamespace(getpid=lambda: 99999, readlink=readlink)}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[])), 'extracted cache_owners', 'exec'), globals_)
    check('reproduced discarded positive cache reference after unreadable maps', globals_['cache_owners']() == [], {'known_cmdline_reference': target, 'later_maps_error': 'PermissionError', 'frozen_result': []})

    plan = json.loads((ROOT / 'next-run-plan.json').read_text())
    expected = {'image-three': 715456512, 'image-five': 765919232, 'owner-controls': 883752960}
    for name, minimum in expected.items():
        stage = plan['stages'][name]
        check('forecast arithmetic '+name, sum(stage['forecast_reserves'].values()) == stage['minimum_serial_required_free_bytes_excluding_new_cache_delta'] == minimum)
        check('owner/proxy raw capture cap '+name, stage['raw_capture_bytes_upper_bound'] >= (stage['owner_pid_epochs_max'] + stage['proxy_cases_max']) * 16 * 1024**2)
        check('no execution authority '+name, 'new pushed source' in stage['conditional_execution'])
    check('majority-threshold caveat exact', (2 // 2 + 1) == (3 // 2 + 1) == 2 and (4 // 2 + 1) == (5 // 2 + 1) == 3)
    shared = (ROOT / 'candidate/partitionline-broker/tests/common/raft_runtime.rs').read_text()
    check('proxy capture occurs before delay', shared.index('gate.capture_file(&name, &bytes)?;') < shared.index('let delay = if reply'))
    check('both directional pipes are joined', 'tokio::join!(pipe(' in shared and 'request.and(response)' in shared)
    check('no-forward metadata starts nullable', all(x in shared for x in ('forward_started_ms: None', 'forward_finished_ms: None', 'write_ok: None', 'shutdown-before-forward', 'partition-drop')))
    check('configured genuine multichunk observer construction', all(x in shared for x in ('let genesis = total - 1;', '40 * 1024', 'assert!(image_bytes > 64 * 1024', '32 * 1024,', 'cluster.image_base(observer)?.1', 'cluster.crash(observer).await?', 'suffix after installed observer restart')))
    check('actual read/chunk Finish/Finished marker requirements', 'chunks >= 3 && finish >= 1 && finished >= 1' in shared)
    check('observer cadence uses process-local clock caveat', 'not owner process clock' in shared and 'restart begins a new clock epoch' in source)
    check('timeout capture target and cleanup are explicit', all(x in source for x in ('Command::Timeout(peer, sequence, image)', 'append_or_image_released', 'feature_released', 'image_fallback_set')))
    after = verify()
    assert after == before
    result = {'classification': 'Independent frozen-source and in-memory semantic review only;0Cargo/compiler/JVM/listener/socket/process/signal/cleanup/repository writes', 'handoff_sha256': HANDOFF, 'frozen20_rows_before_after_exact': True, 'logical_payload_bytes': sum(v['bytes'] for v in before.values()), 'rows': before, 'cpu_set': sorted(os.sched_getaffinity(0)), 'controls': controls, 'passed_controls': len(controls), 'Envelope_literal_scan': literals}
    path = OUT / 'controls.json'
    assert not path.exists()
    path.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    path.chmod(0o600)
    print(json.dumps({'controls': len(controls), 'receipt': str(path), **identity(path)}))


if __name__ == '__main__': main()

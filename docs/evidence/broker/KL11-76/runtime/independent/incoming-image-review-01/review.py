#!/usr/bin/env python3
"""Read-only finite review of actual incoming-image failure and candidate source."""
import difflib
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys

sys.dont_write_bytecode = True
ROOT = Path('/workspace/work/raft-runtime-76')
ACTUAL = ROOT / 'development/incoming-lifetime-ea9ff293-02'
PROPOSAL = ROOT / 'coverage-proposal-01'
OUT = ROOT / 'incoming-image-review-01'
ORACLE = Path('/workspace/partitionline/docs/evidence/broker/KL11-74/oracle/history/membership_raw.py')
FIXED = ORACLE.parents[3] / 'KL11-15/oracle/history/wal_oracle.py'


def need(condition, message):
    if not condition:
        raise ValueError(message)


def info(path, limit=4 * 1024 * 1024):
    st = path.lstat()
    need(stat.S_ISREG(st.st_mode) and not path.is_symlink(), f'regular input: {path}')
    need(st.st_size <= limit, f'input cap: {path}')
    data = path.read_bytes()
    return {'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data),
            'full07777': stat.S_IMODE(st.st_mode)}, data


def inventory(root):
    paths = sorted(p for p in root.rglob('*') if p.is_file())
    need(len(paths) <= 100, 'finite input path count')
    return {str(p.relative_to(root)): info(p)[0] for p in paths}


def cited(path, start, end):
    data = path.read_text().splitlines()
    return {'path': str(path), 'sha256': info(path)[0]['sha256'],
            'start_line': start, 'end_line': end,
            'text': '\n'.join(data[start - 1:end])}


def main():
    need(tuple(sorted(os.sched_getaffinity(0))) == (2, 4), 'review CPU affinity')
    paths = [ACTUAL / 'validation.json', ACTUAL / 'selected-source-manifest.json',
             ACTUAL / 'semantic-reproduction.json', ACTUAL / 'baseline-one-regression/command.log',
             ACTUAL / 'baseline-one-regression/command.exit',
             ACTUAL / 'baseline-one-regression/disk-monitor.jsonl', ORACLE, FIXED,
             PROPOSAL / 'before/partitionline-broker/src/raft/replication.rs',
             PROPOSAL / 'before/partitionline-broker/src/raft/runtime.rs',
             PROPOSAL / 'candidate/partitionline-broker/src/raft/replication.rs',
             PROPOSAL / 'candidate/partitionline-broker/src/raft/runtime.rs',
             PROPOSAL / 'candidate/partitionline-broker/tests/raft_membership.rs',
             PROPOSAL / 'failing-first-overlay/partitionline-broker/tests/raft_membership.rs',
             PROPOSAL / 'syntax-stage-02/replication.rs.patch']
    before = {str(p): info(p)[0] for p in paths}
    expected = {
        str(paths[0]): 'f1876594e68aa118df6de7c6eae6e679e158759d6f2fb5738a573738cb3636c4',
        str(paths[8]): '430f3fb9be194ab304b5328afa1cd398d2d7fac1c8c6070f06409707566e3c3f',
        str(paths[10]): '47a9b89cc7f0c24e667f0ba8433d1b6beca3fdf2c6692099747ac80c0a5020e0',
        str(paths[14]): 'edfa8b69563acfb41b741f788681c5ce836a57bbda322dedfd9b56de6f7357ea'}
    for p, digest in expected.items():
        need(before[p]['sha256'] == digest, f'pinned input SHA: {p}')
    validation = json.loads(paths[0].read_bytes())
    need(validation['source_commit'] == 'ea9ff29393526b57e1203612bb12c9c99deccc61', 'actual source')
    commands = validation['commands']
    need(len(commands) == 1, 'one actual command only')
    cmd = commands[0]
    need(cmd['exit_code'] == 101 and cmd['name'] == 'baseline-one-regression', 'actual failure status')
    log = paths[3].read_text()
    need(before[str(paths[3])]['sha256'] == cmd['log_sha256'], 'command log binding')
    for fragment in ['running 1 test', 'Error: InvalidPeer',
                     '0 passed; 1 failed; 0 ignored; 0 measured; 27 filtered out']:
        need(fragment in log, 'actual log: ' + fragment)
    need(paths[4].read_text().strip() == '101', 'actual command.exit')
    need(cmd['source_before'] == cmd['source_after'] == cmd['source_after_retention'] ==
         validation['final_source_guards'], 'recorded original+selected source guards')
    mon = [json.loads(line) for line in paths[5].read_text().splitlines()]
    dm = cmd['disk_monitor']
    need(before[str(paths[5])]['sha256'] == dm['sample_log_sha256'], 'raw monitor SHA')
    need(len(mon) == dm['samples'] == 28 and mon[0]['pre_launch'] and mon[-1]['process_completed'],
         'raw monitor phases/count')
    need(min(row['free_bytes'] for row in mon) == dm['minimum_free_bytes'] == 900546560,
         'actual disk minimum')
    need(not dm['triggered'] and dm['trigger'] is None and dm['actual_process_exit_code'] == 101 and
         all(not row['below_reserve'] and row['free_bytes'] >= 367001600 for row in mon), 'no disk refusal')

    manifest = json.loads(paths[1].read_bytes())
    source = Path(validation['source_tree'])
    actual_source_before = inventory(source)
    need(len(manifest) == len(actual_source_before) == 66 and set(manifest) == set(actual_source_before),
         'selective66 exact pathset')
    for relative, row in manifest.items():
        metadata, data = info(source / relative)
        need(metadata == {'sha256': row['sha256'], 'bytes': row['bytes'],
                          'full07777': row['full_permission_mode']}, 'selective row identity: ' + relative)
        blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        need(blob == row['git_blob_sha1'], 'selected manifest blob identity: ' + relative)
    need(info(source / 'partitionline-broker/src/raft/replication.rs')[0] == before[str(paths[8])],
         'actual old replication exact bytes/mode')
    need(info(source / 'partitionline-broker/src/raft/runtime.rs')[0] == before[str(paths[9])],
         'actual old runtime exact bytes/mode')
    need(info(source / 'partitionline-broker/tests/raft_membership.rs')[0] == before[str(paths[13])],
         'actual regression exact declared overlay')

    archive = ACTUAL / cmd['executed_elfs'][0]['retained_object']
    archive_before, _ = info(archive)
    harness = cmd['executed_elfs'][0]
    need(len(cmd['executed_elfs']) == 1 and harness['original_mode'] == 0o700, 'one actual harness mode')
    with gzip.open(archive, 'rb') as stream:
        decoded = stream.read(harness['bytes'] + 1)
    need(len(decoded) == harness['bytes'] == 3597232 and decoded.startswith(b'\x7fELF') and
         hashlib.sha256(decoded).hexdigest() == harness['sha256'], 'retained failed harness exact bytes')
    retained = [r for r in cmd['post_command_elfs'] if r['sha256'] == harness['sha256']]
    need(len(retained) == 1 and retained[0]['pre_clean_bytes_and_gzip_verified'] and
         retained[0]['original_mode'] == harness['original_mode'], 'post-command fullmode preservation row')
    del decoded

    raw_before = inventory(ACTUAL / 'captures')
    need(len(raw_before) == 33 and set(raw_before) == set(validation['captured_files']), 'actual33 paths')
    for relative, metadata in raw_before.items():
        claimed = validation['captured_files'][relative]
        need(metadata == {'sha256': claimed['sha256'], 'bytes': claimed['bytes'],
                          'full07777': claimed['full_mode']}, 'actual raw capture binding: ' + relative)
    phases = ['before-poll', 'after-poll', 'after-first-chunk']
    durable = []
    facts = {}
    for phase in phases:
        rows = {p[len(phase) + 1:]: row for p, row in raw_before.items()
                if p.startswith(phase + '/') and not p.endswith('actual-facts.txt')}
        need(len(rows) == 10, 'ten durable files per phase')
        durable.append(rows)
        values = {}
        for line in (ACTUAL / 'captures' / phase / 'actual-facts.txt').read_text().splitlines()[1:]:
            k, v = line.split('=', 1)
            values[k] = v
        facts[phase] = values
    need(durable[0] == durable[1] == durable[2], 'all30 durable bytes/fullmode/pathset unchanged')
    need(facts[phases[0]]['outcome'] == 'valid Begin completed' and
         facts[phases[1]]['outcome'] == 'unchanged Follower poll completed' and
         facts[phases[2]]['outcome'] == 'Err(InvalidPeer)', 'actual ordered outcomes')
    need(len({facts[p]['offer'] for p in phases}) == len({facts[p]['receiver_state'] for p in phases}) == 1,
         'same offer/observable receiver state across phases')
    need(all(facts[p]['receiver_vote'] == 'None' for p in phases), 'actual weak None-vote coverage')

    spec = importlib.util.spec_from_file_location('incoming_review_membership_raw', ORACLE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    decoded_phases = {}
    for phase in phases:
        root = ACTUAL / 'captures' / phase
        rows = {}
        for node in range(1, 5):
            stem = f'{node}-' + bytes([node + 1] * 16).hex()
            content = module.read_content(root / (stem + '.wal'), root / (stem + '.images'))
            election = module.read_election(root / (stem + '.election'), content)
            rows[str(node)] = {'local': content.local, 'group': content.group,
                'view': content.view, 'tail': content.tail(), 'committed': content.committed,
                'max_authority_term': content.max_term, 'selected': content.selected,
                'records': [record.receipt() for record in content.rows],
                'operations': content.operations, 'election': election}
        decoded_phases[phase] = rows
        observer = rows['4']
        need(observer['tail'] == (0, 0) and observer['committed'] == 0 and
             observer['selected'] is None and len(observer['operations']) == 1 and
             observer['election']['final']['term'] == 2 and
             observer['election']['final']['vote'] is None and observer['election']['state_count'] == 3,
             'raw observer context: empty WAL/no Install, term2 None vote')
        selected = rows['1']['selected']
        need(selected == {'generation': '3d' * 16, 'base': {'term': 2, 'index': 2},
                          'records': 2, 'payload_bytes': 23, 'bytes': 431, 'checksum': 729818836},
             'raw offered descriptor equals actual leader selected image')
        need([r['index'] for r in rows['1']['records']] == [1, 2] and
             rows['1']['records'][1]['payload_hex'] == b'committed image payload'.hex(),
             'raw leader committed image exact payload')
        partial = root / ('4-' + '05' * 16 + '.images') / ('snapshot-' + '3d' * 16 + '.partial')
        need(partial.stat().st_size == 0, 'first Chunk did not write partial payload')
    need(decoded_phases[phases[0]] == decoded_phases[phases[1]] == decoded_phases[phases[2]],
         'independent decoded durable states identical')

    old = paths[8].read_text().splitlines(keepends=True)
    candidate = paths[10].read_text().splitlines(keepends=True)
    generated = ''.join(difflib.unified_diff(old, candidate,
        fromfile='a/partitionline-broker/src/raft/replication.rs',
        tofile='b/partitionline-broker/src/raft/replication.rs'))
    need(generated == paths[14].read_text(), 'patch exactly represents old -> candidate')

    excerpts = [cited(paths[8], 1858, 1894), cited(paths[10], 1858, 1910),
                cited(paths[10], 2189, 2206), cited(paths[10], 2298, 2309),
                cited(paths[10], 2400, 2429), cited(paths[10], 4064, 4080),
                cited(paths[10], 687, 706), cited(paths[10], 4542, 4621),
                cited(paths[11], 1200, 1244), cited(paths[11], 1364, 1385),
                cited(paths[13], 1895, 1960), cited(paths[12], 1916, 2095)]
    after = {str(p): info(p)[0] for p in paths}
    need(before == after and raw_before == inventory(ACTUAL / 'captures') and
         actual_source_before == inventory(source) and archive_before == info(archive)[0],
         'all inspected sources/raw/logs/retained harness unchanged')
    receipt = {'schema_version': 1, 'task': 'KL11-76 incoming-image finite read-only review',
        'scope': 'one actual selective66 baseline run plus independent byte decoding and candidate static source review; no candidate execution or full task qualification',
        'review_affinity': [2, 4], 'actual_new_Cargo_SDK_runtime_invocations': 0,
        'source_commit': validation['source_commit'], 'actual_validation': before[str(paths[0])],
        'command': {k: cmd[k] for k in ['name', 'argv', 'exit_code', 'elapsed_seconds', 'source_before', 'source_after', 'source_after_retention', 'log_sha256', 'disk_monitor']},
        'complete_source_guards': {'scope': 'original full73171 guards are retained command provenance, not independently rerun by this finite review; actual selective66 bytes/full07777/Git blob identities and pathset independently rechecked'},
        'failed_harness': {**harness, 'archive_path': str(archive), 'archive_metadata': archive_before,
                           'independently_decompressed_verified': True,
                           'post_command_retention': retained[0]},
        'input_before': before, 'input_after': after, 'actual_raw_files': raw_before,
        'actual_source_66_files': actual_source_before, 'all_inputs_unchanged': True,
        'facts': facts, 'independent_decoded_phases': decoded_phases,
        'candidate_patch_exact_old_to_new': True, 'source_excerpts': excerpts,
        'findings': [
          {'id': 'F1', 'conclusion': 'Actual baseline lifetime defect reproduced',
           'support': 'valid Begin at16 -> unchanged follower poll17 -> first Chunk at17 InvalidPeer; raw empty partial/no Install and unchanged WAL/election/image bytes support no durable mutation. Private dynamic.incoming is not emitted; its clearing is established by old source and exact call order.'},
          {'id': 'F2', 'conclusion': 'Narrow candidate context preservation is justified; no static safety blocker found',
           'support': 'reset_authority preserves only an existing exact-context incoming transfer with ready follower, same term/current leader, eligible full leader directory, and unexpired deadline; outgoing leadership ownership still clears. Chunk/Finish compare stored Context and entire SnapshotRequest before authority-invalidating abort, so mismatched messages cannot reach that destructive abort.'},
          {'id': 'F3', 'conclusion': 'Current authority and deadline fences remain; configuration epoch is not current-view equality',
           'support': 'validate_context validates local full peer key and eligible full leader key; authority_key permits current voter or previous voter during pending same-term configuration. Exact offered Context equality prevents altered-epoch forgery. Real term/leader change or expiry (>deadline) aborts via incoming/poll; role/candidate changes clear context through controller_result/update_authority.'},
          {'id': 'F4', 'conclusion': 'Possible bounded cleanup delay requires explicit expected policy, not a proved safety defect',
           'support': 'If leader directory eligibility changes without term/leader/deadline change, reset_authority clears dynamic context while poll only aborts incoming_snapshot for those three listed conditions. The staged transfer may remain unavailable/Busy until explicit abort, disconnect, deadline or drop. No execution proves this path in this baseline.'},
          {'id': 'F5', 'conclusion': 'Candidate fences are source-only; baseline does not prove completion or nonempty vote preservation',
           'support': 'Actual test stops at first Chunk, before Finish/ACK/install/reopen. Observer vote is None. Candidate four tests use broad is_err for negative cases and small431B image; candidate Cargo/runtime has not been executed by this review.'}],
        'acceptance_test_recommendations': [
            'Run corrected positive Begin->poll->multiple Chunks with intervening polls->Finish/Finished->durable image/WAL/election receipts->reopen; retain raw before/after completion.',
            'Use actual non-null same-term voted_directory fixture and verify that vote is preserved across install and reopen.',
            'For changed Context/request.peer/leader/term/sequence/leader_commit and every descriptor field, assert typed InvalidPeer and ready/unpoisoned plus unchanged durable counters, then finish the unchanged legitimate transfer.',
            'Test deadline equality permits current transfer and deadline+1 rejects with partial cleanup; compare same process monotonic clock only.',
            'Exercise role/campaign and real known higher-term leader fences; explicitly test leader removal/configuration eligibility and document any staged cleanup delay.',
            'Keep lagging configuration_epoch/pending previous-leader policy distinct from altered offered Context rejection; do not invent current epoch equality requirement.',
            'Actual large multichunk TCP Finish/Finished, non-genesis observer install, timeout targets/errors, resource peaks and joined lifecycle remain separate76 qualification gaps.'],
        'limits': ['Finite retained history and static source review, not exhaustive consensus proof.',
                   'No candidate execution, SDK compatibility, power-loss, throughput, or complete KL11-76 closure claim.',
                   'No source/raw/receipt/ELF file changes; only this new WORK script and reviewer receipt created.']}
    data = json.dumps(receipt, sort_keys=True, indent=2).encode() + b'\n'
    need(len(data) <= 2 * 1024 * 1024, 'review receipt <=2MiB')
    output = OUT / 'validation.json'
    need(not output.exists(), 'fresh reviewer receipt')
    with output.open('xb') as stream:
        stream.write(data)
    output.chmod(0o600)
    print(json.dumps({'receipt': str(output), 'sha256': hashlib.sha256(data).hexdigest(),
                      'bytes': len(data), 'full07777': 0o600, 'raw_files': 33,
                      'decoded_node_phases': 12, 'actual_new_Cargo_SDK_runtime_invocations': 0}))


if __name__ == '__main__':
    main()

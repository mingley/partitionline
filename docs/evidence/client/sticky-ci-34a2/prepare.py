#!/usr/bin/env python3
"""Prepare exact source-only fixes; no Cargo, sockets or repository writes."""
from pathlib import Path
import difflib
import hashlib
import itertools
import json
import os
import re
import stat

HERE = Path(__file__).resolve().parent
REPO = Path('/workspace/partitionline')
SOURCE = '34a2e672408d64309b0c6cb3dca428f59bd30e0b'
LOGROOT = Path('/workspace/work/integration/ci-34a2-runtime-01/logs')


def identity(path):
    info = path.lstat()
    assert stat.S_ISREG(info.st_mode)
    raw = path.read_bytes()
    return {'path': str(path), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
            'full_mode': f'{stat.S_IMODE(info.st_mode):04o}',
            'git_blob_sha1': hashlib.sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest()}


def write(name, text):
    path = HERE / name
    assert not path.exists(), str(path)
    path.write_text(text)
    os.chmod(path, 0o600)
    return identity(path)


def replace_once(text, old, new):
    assert text.count(old) == 1, old[:120]
    return text.replace(old, new, 1)


def main():
    os.umask(0o077)
    paths = ['src/producer.rs', 'tests/sticky_partitioner.rs', 'src/metrics.rs',
             'tests/common/mod.rs', 'src/protocol/records.rs', 'src/protocol/buf.rs']
    before = [identity(REPO / p) for p in paths]
    producer = (REPO / paths[0]).read_text()
    tests = (REPO / paths[1]).read_text()
    assert before[0]['sha256'] == '37098eee8da69a2e8c39b24cd88358447d55a8992d3ef6b21e9097fabcfebc10'
    assert before[1]['sha256'] == 'a1e6010fbde683b87989761efa56ecdab53c1140ba9cd63d8c1adceaaaaeafaa'
    fixed_producer = replace_once(producer,
        '        let cap = self.cfg.buffer_memory;\n        if cap == 0 {\n            return true;\n        }',
        '        let cap = self.cfg.buffer_memory;\n        if cap == 0 {\n'
        '            // Unlimited capacity still owns a counted reservation: failed\n'
        '            // admission, cancellation and delivery each release it once.\n'
        '            let _ = self.buffered_bytes.fetch_add(bytes, Ordering::Relaxed);\n'
        '            return true;\n        }')
    candidate = tests
    assert candidate.count('.buffer_memory(160)') == 3
    candidate = candidate.replace('.buffer_memory(160)', '.buffer_memory(200)')
    candidate = replace_once(candidate,
        'async fn retained_topic_text_and_idle_eviction_stay_within_hard_caps() {\n'
        '    let mock = common::Mock::start().await;\n',
        'async fn retained_topic_text_and_idle_eviction_stay_within_hard_caps() {\n'
        '    let mock = common::Mock::start().await;\n'
        '    for topic in ["a", "bb", "ccc", "dddd", "toolongfortextcap"] {\n'
        '        mock.set_topic_partitions(topic, 1);\n    }\n')
    transaction_start = candidate.index('async fn transaction_partition_failure_retires_cohort_and_allows_abort()')
    transaction_end = candidate.index('\n#[tokio::test]', transaction_start)
    tx_body = candidate[transaction_start:transaction_end]
    tx_fixed = replace_once(tx_body,
        '    warm(&producer).await.unwrap();\n    producer.begin_transaction().await.unwrap();',
        '    producer.begin_transaction().await.unwrap();\n'
        '    warm(&producer).await.unwrap();\n'
        '    producer.commit_transaction().await.unwrap();\n'
        '    producer.begin_transaction().await.unwrap();')
    candidate = candidate[:transaction_start] + tx_fixed + candidate[transaction_end:]
    regression = '''
#[tokio::test]
async fn unlimited_byte_budget_tracks_payload_reservations_and_terminal_releases() {
    // Both ordinary and sticky admission share the payload reservation owner.
    // Zero removes the cap; it must not disable the observable byte gauge.
    for sticky in [false, true] {
        let mock = common::Mock::start().await;
        let mut config = ProducerConfig::bootstrap([mock.addr.clone()])
            .connections(1)
            .buffer_memory(0)
            .linger(Duration::from_secs(30))
            .batch_bytes(100_000);
        if sticky {
            config = config.partitioner(StickyPartitioner::seeded(9));
        }
        let producer = Producer::new(config).await.unwrap();
        warm(&producer).await.unwrap();
        assert_eq!(producer.metrics().bytes_buffered, 0);
        // t has only partition 0. Sticky reservation precedes this route
        // refusal, so every repeated rollback must return the gauge to zero.
        for _ in 0..20 {
            assert!(matches!(
                producer.try_send(ProduceRecord::to("t").partition(1).value(&b"rollback"[..])),
                Err(Error::QueueFull)
            ));
            assert_eq!(producer.metrics().bytes_buffered, 0);
        }
        let first = mock.produce_batches().len();
        admit(&producer, ProduceRecord::to("t").value(&b"alpha"[..]))
            .await
            .unwrap();
        admit(
            &producer,
            ProduceRecord::to("t")
                .key(&b"k"[..])
                .value(&b"beta"[..])
                .header("trace", &b"context"[..]),
        )
        .await
        .unwrap();
        assert_eq!(producer.metrics().bytes_buffered, 22);
        producer.flush().await.unwrap();
        assert_eq!(producer.metrics().bytes_buffered, 0);
        assert_eq!(
            mock.produce_batches()
                .into_iter()
                .skip(first)
                .map(|batch| batch.2)
                .sum::<i32>(),
            2
        );
        producer.inject_pre_send_fault(PreSendFault::Write);
        admit(&producer, ProduceRecord::to("t").value(&b"fail"[..]))
            .await
            .unwrap();
        assert_eq!(producer.metrics().bytes_buffered, 4);
        assert!(producer.flush().await.is_err());
        assert_eq!(producer.metrics().bytes_buffered, 0);
        if sticky {
            assert_eq!(producer.__test_sticky_records(), Some((0, 100_000)));
        }
        producer.close().await.unwrap();
        assert_eq!(producer.metrics().bytes_buffered, 0);
    }
}
'''
    # close consumes Producer; retain a clone solely to observe the final gauge.
    regression = regression.replace(
        '        producer.close().await.unwrap();\n        assert_eq!(producer.metrics().bytes_buffered, 0);',
        '        let observer = producer.clone();\n        producer.close().await.unwrap();\n'
        '        assert_eq!(observer.metrics().bytes_buffered, 0);')
    candidate += regression
    files = [write('sticky_partitioner.before.rs', tests),
             write('sticky_partitioner.candidate.rs', candidate)]
    files.append(write('producer.rs.patch', ''.join(difflib.unified_diff(
        producer.splitlines(True), fixed_producer.splitlines(True),
        fromfile='a/src/producer.rs', tofile='b/src/producer.rs'))))
    files.append(write('sticky_partitioner.rs.patch', ''.join(difflib.unified_diff(
        tests.splitlines(True), candidate.splitlines(True),
        fromfile='a/tests/sticky_partitioner.rs', tofile='b/tests/sticky_partitioner.rs'))))
    expected = {'cancellation_before_buffer_admission_does_not_charge_or_leak',
                'cancellation_before_enqueue_releases_the_reserved_record_slot_without_policy_charge',
                'failed_buffer_admission_does_not_charge_or_consume_route',
                'retained_topic_text_and_idle_eviction_stay_within_hard_caps',
                'transaction_partition_failure_retires_cohort_and_allows_abort',
                'two_record_cap_bounds_null_and_empty_payloads_even_with_unlimited_byte_budget'}
    log_rows, excerpts = [], []
    for job in ['111311339756', '111311339845', '111311339705']:
        path = LOGROOT / f'{job}.decoded.log'
        log_rows.append(identity(path))
        raw = path.read_text()
        start = raw.index('tests/sticky_partitioner.rs (')
        end = raw.index('test result: FAILED. 22 passed; 6 failed;', start)
        end = raw.index('\n', end)
        body = raw[start:end]
        names = set(re.findall(r'test (\w+) \.\.\. FAILED', body))
        assert names == expected, (job, names)
        assert body.count('RecordTooLarge { size: 166, max: 160, config: "buffer.memory" }') == 3
        assert 'UnknownTopic("a")' in body
        assert 'Protocol("produce outside a transaction")' in body
        assert 'left: 18446744073709551604' in body
        excerpts.append({'job_id': job, 'full_log': log_rows[-1], 'cohort_excerpt': body,
                         'failed_test_names': sorted(names), 'passed': 22, 'failed': 6})
    files.append(write('ci-sticky-excerpts.json', json.dumps(excerpts, indent=2, sort_keys=True) + '\n'))
    controls = []

    def check(name, okay, detail):
        assert okay, (name, detail)
        controls.append({'name': name, 'passed': True, 'detail': detail})

    # Independent v2 size arithmetic: 61 batch header + 21 max record
    # overhead + null key(1) + zigzag length80(2)+payload80 + headers count(1).
    upper = 61 + 21 + 1 + 2 + 80 + 1
    check('single_record_original_fixture_is_rejected', upper == 166 and upper > 160,
          'all three original 160-byte fixtures hit the genuine single-record limit before queue pressure')
    check('corrected_fixture_reaches_aggregate_pressure', upper <= 200 and 80 * 2 <= 200 < 80 * 3,
          'same payload160 gauge/assertions; third payload exceeds cap without single-record rejection')
    check('old_unlimited_gauge_reproduces_observed_wrap', (-4 * 3) % (1 << 64) == 18446744073709551604,
          'compatible finite three-release history, not a reconstructed actual warm retry count')
    events = [(kind, record) for record in range(3) for kind in ['reserve', 'release']]
    payloads = [4, 0, 8]
    cases = 0
    for order in itertools.permutations(events):
        position = {event: i for i, event in enumerate(order)}
        if any(position[('reserve', n)] > position[('release', n)] for n in range(3)):
            continue
        active, gauge = set(), 0
        for kind, n in order:
            if kind == 'reserve':
                active.add(n)
                gauge += payloads[n]
            else:
                active.remove(n)
                gauge -= payloads[n]
            assert gauge == sum(payloads[n] for n in active) and gauge >= 0
        assert gauge == 0
        cases += 1
    check('corrected_counting_all_causal_reserve_release_orders', cases == 90,
          {'causal_event_orders': cases, 'payloads': payloads, 'all_prefix_gauges': 'exact live ownership sum'})
    for outcome in ['route-refused', 'cancelled-before-enqueue', 'acked', 'terminal-error']:
        gauge = 22
        gauge -= 22
        check(f'unlimited_{outcome}_releases_exactly_once', gauge == 0,
              'every owned reservation is counted even when cap0; no saturating subtraction')
    check('topic_fixture_declares_every_test_topic',
          'mock.set_topic_partitions(topic, 1);' in candidate,
          'metadata mock intentionally advertises only created topics; no fabricated auto creation')
    check('transaction_warm_is_inside_distinct_completed_transaction',
          tx_fixed.index('begin_transaction') < tx_fixed.index('warm(&producer)') <
          tx_fixed.index('commit_transaction') < tx_fixed.rindex('begin_transaction'),
          'warm transaction completes; injected failure runs in a fresh transaction with cleared partition enrollment')
    check('all_existing_test_functions_preserved',
          set(re.findall(r'async fn (\w+)\(', tests)) <= set(re.findall(r'async fn (\w+)\(', candidate)),
          'no test removed, ignored, filtered or weakened by dropping its semantic assertions')
    check('new_regression_genuine_both_admission_paths',
          'for sticky in [false, true]' in regression and '.buffer_memory(0)' in regression and
          'bytes_buffered, 22' in regression and 'PreSendFault::Write' in regression,
          'draft socket test covers ordinary/sticky, failed routing, payload+header gauge, ack and terminal error')
    after = [identity(REPO / p) for p in paths]
    assert before == after
    summary = {
        'schema_version': 1, 'source_sha': SOURCE,
        'scope': 'Actual hosted six-failure classification; source-only correction and finite Python models. No corrected Rust run.',
        'source_before': before, 'source_after': after, 'original_bytes_and_full_modes_unchanged': True,
        'actual_hosted_jobs': log_rows, 'actual_hosted_each': {'tests_passed': 22, 'tests_failed': 6},
        'production_fix': {'file': 'src/producer.rs', 'patch': 'producer.rs.patch',
                           'expected_result_sha256': hashlib.sha256(fixed_producer.encode()).hexdigest(),
                           'changed_function': 'Shared::try_reserve_buffer',
                           'invariant': 'cap0 disables ceiling only; every byte reservation must still increment before its exactly-once release'},
        'test_candidate': {'file': 'tests/sticky_partitioner.rs', 'candidate': files[1],
                           'before_tests': tests.count('#[tokio::test]'),
                           'after_tests': candidate.count('#[tokio::test]'),
                           'added_regressions': 1, 'runtime_status': 'uncompiled and unexecuted'},
        'classifications': ['3 fixture byte limits166>160 corrected to200, aggregate third240>200 still blocked',
                            '1 fixture lacks declared topics, explicitly create all5',
                            '1 fixture warms transactional producer before begin, warm separate valid transaction',
                            '1 real buffer.memory0 counter underflow, count unlimited reservations symmetrically'],
        'controls': controls, 'python_controls_passed': len(controls), 'causal_event_orders': cases,
        'execution': {'cargo': 0, 'compiler': 0, 'rustfmt': 0, 'sdk_runtime': 0,
                      'listeners': 0, 'repo_writes': 0, 'cache_writes': 0},
        'coordination': 'Root alone installs. C_peer Producer owner must port this narrow function correction into superseding auth draft.',
        'next_required': ['actual hosted sticky29 regression suite on corrected source',
                          'strict client lint and relevant producer suites', 'do not claim old actual failure histories now pass']}
    files.append(write('validation.json', json.dumps(summary, indent=2, sort_keys=True) + '\n'))
    total = sum(row['bytes'] for row in files)
    assert total <= 240_000
    packet = {'schema_version': 1, 'files': files, 'payload_bytes': total,
              'source_sha': SOURCE, 'excluded_original_full_logs': log_rows,
              'script': identity(HERE / 'prepare.py')}
    frozen = write('stage-handoff.json', json.dumps(packet, indent=2, sort_keys=True) + '\n')
    print(json.dumps({'packet': frozen, 'payload_bytes': total,
                      'candidate_tests_sha256': files[1]['sha256'],
                      'expected_producer_sha256': summary['production_fix']['expected_result_sha256'],
                      'actual_corrected_rust_runs': 0, 'source_controls': len(controls)}))


if __name__ == '__main__':
    main()

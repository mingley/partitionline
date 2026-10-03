"""Construct immutable input events; do not manufacture Java expected output."""
from pathlib import Path
import hashlib
import json

ROOT = Path(__file__).resolve().parent
MASK = (1 << 64) - 1


def draws(seed, count=64):
    values = []
    for _ in range(count):
        seed = (seed + 0x9e3779b97f4a7c15) & MASK
        z = seed
        z = ((z ^ (z >> 30)) * 0xbf58476d1ce4e5b9) & MASK
        z = ((z ^ (z >> 27)) * 0x94d049bb133111eb) & MASK
        z ^= z >> 31
        values.append(z & 0x7fffffff)
    return values


def append(kind, alias, record_bytes, delta, full, partition=-1):
    # The public Record bound with null/empty key and no headers is max21 plus
    # nullable key length1, value length varint, value bytes, header count1.
    value_bytes = record_bytes - 25
    assert value_bytes >= 64 and record_bytes == 25 + value_bytes
    return [kind, alias, partition, value_bytes, record_bytes, delta, int(full)]


def cleanup(*aliases):
    return [['R', alias] for alias in aliases]


same_seed = next(seed for seed in range(1000) if draws(seed, 2)[0] % 3 == draws(seed, 2)[1] % 3)
mix_seed = 9
mix_partition = draws(mix_seed, 1)[0] % 3
cases = [
    ('exact_B_actual_cohort_full', 300, 100, 3, 7, 1,
     [append('U', 'a', 139, 200, False), append('U', 'b', 100, 100, True),
      ['D', 'a', 1], *cleanup('a', 'b')]),
    ('B_deferred_until_all_real_tails_sealed', 300, 100, 3, 7, 1,
     [append('U', 'a', 139, 200, False), append('U', 'b', 139, 200, False),
      ['D', 'a', 0], ['D', 'b', 1], *cleanup('a', 'b')]),
    ('two_B_forces_switch_with_incomplete_tail', 300, 100, 3, 7, 1,
     [append('U', 'a', 139, 200, False), append('U', 'b', 139, 200, False),
      append('U', 'c', 139, 200, False), ['D', 'c', 1], ['D', 'a', 1],
      ['D', 'b', 1], *cleanup('a', 'b', 'c')]),
    ('same_partition_redraw_is_new_generation', 300, 100, 3, 7, same_seed,
     [append('U', 'a', 239, 300, True), ['P'], ['D', 'a', 1], *cleanup('a')]),
    ('one_partition_still_rotates_generation', 100, 100, 1, 1, 9,
     [append('U', 'a', 100, 161, True), ['P'], ['D', 'a', 1], *cleanup('a')]),
    ('only_leader_eligible_indices_at_new_draws', 300, 100, 6, 21, 79443,
     [append('U', 'a', 239, 300, True), append('U', 'b', 239, 300, True),
      append('U', 'c', 239, 300, True), ['D', 'a', 1], ['D', 'b', 1],
      ['D', 'c', 1], *cleanup('a', 'b', 'c')]),
    ('all_unavailable_state_machine_fallback_only', 300, 100, 3, 0, 9,
     [append('U', 'a', 239, 300, True), ['D', 'a', 1], *cleanup('a')]),
    ('partial_drain_below_B_preserves_counter', 300, 100, 3, 7, 1,
     [append('U', 'a', 100, 161, False), ['D', 'a', 1], ['P'], *cleanup('a')]),
    ('mixed_empty_key_participates_without_unkeyed_charge', 300, 100, 3, 7, mix_seed,
     [append('U', 'a', 100, 161, False), append('K', 'k', 100, 100, False, mix_partition),
      append('U', 'b', 100, 161, False), ['D', 'a', 0], ['D', 'b', 1],
      *cleanup('a', 'k', 'b')]),
    ('explicit_tail_owns_initial_overhead', 300, 100, 3, 7, mix_seed,
     [append('E', 'e', 100, 161, False, mix_partition), append('U', 'a', 100, 100, False),
      ['D', 'e', 1], *cleanup('e', 'a')]),
    ('record_count_seals_true_tail', 1000, 2, 3, 7, 9,
     [append('U', 'a', 100, 161, False), append('U', 'b', 100, 100, True),
      ['D', 'a', 1], *cleanup('a', 'b')]),
    ('leader_loss_and_failed_peeks_are_pure', 300, 100, 3, 7, 9,
     [append('U', 'a', 100, 161, False), ['M', 0], ['P'], ['F'], ['M', 7],
      ['D', 'a', 1], *cleanup('a'), append('U', 'b', 100, 161, False),
      ['D', 'b', 1], *cleanup('b')]),
]
lines = ['# uniform-rust-packed-events-v1',
         '# CASE name batch_bytes batch_records partitions availability_mask rust_seed_hex controlled_java_draws',
         '# U/K/E alias route_partition value_length expected_record_bound expected_packed_delta expected_full',
         '# D alias expected_full; R alias; P/F pure peek/failed plan; M availability_mask']
events = 0
for name, batch, records, partitions, available, seed, script in cases:
    raw = draws(seed)
    # Reference inputs intentionally avoid the Rust rejection edge. Exact unbiased
    # Rust rejection and failed-admission state are covered by separate real tests.
    for count in range(1, partitions + 1):
        accepted = (1 << 31) - (1 << 31) % count
        assert all(value < accepted for value in raw)
    lines.append('\t'.join(map(str, ['CASE', name, batch, records, partitions,
                                     available, f'{seed:016x}', ','.join(map(str, raw))])))
    lines.extend('\t'.join(map(str, event)) for event in script)
    events += len(script)
data = ('\n'.join(lines) + '\n').encode()
assert len(data) <= 32768 and events <= 256 and len(cases) <= 32
(ROOT / 'uniform-input.tsv').write_bytes(data)
(ROOT / 'input-provenance.json').write_text(json.dumps({
    'schema': 1, 'status': 'Prepared immutable input only; Java fixture not generated or synthesized',
    'cases': len(cases), 'events': events, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
    'seed_draws': 'Offline fixed SplitMix64 arithmetic produces accepted Java override values; no Java RNG identity and no production execution',
    'record_accounting': 'Independently prescribed public Record upper-bound bytes plus61 only when a new Rust cohort opens; Rust fixture consumer must assert actual plan.delta and actual tail state against input',
    'scope': 'One topic per case, uniform adaptive=false transitions and Rust explicit batch notifications. All-unavailable case is policy selection only, not acquired/routed live delivery.',
    'nonclaims': ['Java RecordAccumulator/compressed-size identity', 'Java RNG identity or exact modulo bias equivalence', 'Whole-producer partition-history equivalence', 'Live broker proof', 'Performance'],
    'case_names': [case[0] for case in cases],
}, indent=2) + '\n')
print(json.dumps({'prepared_cases': len(cases), 'prepared_events': events,
                  'input_bytes': len(data), 'input_sha256': hashlib.sha256(data).hexdigest(),
                  'Java_executed': False, 'Rust_executed': False}, indent=2))

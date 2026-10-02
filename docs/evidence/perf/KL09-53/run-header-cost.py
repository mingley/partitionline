#!/usr/bin/env python3
"""Baseline-only cost visibility study; no candidate or instruction claim."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import subprocess

CASES = ['header-classic', 'header-flexible', 'micro-request-v9-body', 'frame-classic', 'frame-flexible']

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def quantile(values, p):
    values = sorted(values)
    pos = (len(values) - 1) * p
    lo = int(pos)
    hi = min(lo + 1, len(values) - 1)
    return values[lo] * (1 - (pos - lo)) + values[hi] * (pos - lo)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path, required=True, help='Clean checkout at the pinned baseline')
    parser.add_argument('--target-dir', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--cpu', type=int, required=True)
    parser.add_argument('--build-cpus', default='0-2,4')
    parser.add_argument('--repetitions', type=int, default=5)
    parser.add_argument('--header-iterations', type=int, default=10_000_000)
    parser.add_argument('--body-iterations', type=int, default=100_000)
    parser.add_argument('--skip-build', action='store_true')
    args = parser.parse_args()
    if min(args.repetitions, args.header_iterations, args.body_iterations) < 1:
        parser.error('counts must be positive')
    repo = args.repo_root.resolve()
    target = args.target_dir.resolve()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).resolve().with_name('header-cost-probe.rs')
    binary = target / 'header-cost-probe'
    commands = []
    def command(argv, cwd=repo, log=None):
        result = subprocess.run(argv, cwd=cwd, text=True, capture_output=True)
        commands.append({'argv':argv, 'cwd':str(cwd), 'exit_code':result.returncode})
        if log:
            (out / log).write_text(result.stdout + result.stderr)
        if result.returncode:
            raise SystemExit(f'command failed with exit {result.returncode}: {argv[0]}')
        return result.stdout
    source_sha = command(['git','rev-parse','HEAD']).strip()
    dirty = command(['git','status','--porcelain']).strip()
    if dirty:
        raise SystemExit('baseline checkout is dirty')
    if not args.skip_build:
        command(['taskset','-c',args.build_cpus,'cargo','build','--locked','--release','--jobs','2','--manifest-path',str(repo/'benchmarks/codec/Cargo.toml'),'--lib','--target-dir',str(target)], log='library-build.txt')
        deps = target / 'release/deps'
        compile_command = ['taskset','-c',args.build_cpus,'rustc','--edition=2021','-O','-C','debuginfo=1',str(source),'-L',f'dependency={deps}','-o',str(binary)]
        for name in ['codec','partitionline','bytes','serde_json','sha2']:
            matches = sorted(deps.glob(f'lib{name}-*.rlib'), key=lambda p:p.stat().st_mtime)
            if not matches:
                raise SystemExit(f'missing {name} release library')
            compile_command += ['--extern',f'{name}={matches[-1]}']
        command(compile_command, log='probe-build.txt')
    if not binary.is_file():
        raise SystemExit('missing probe binary')
    records = []
    for rep in range(args.repetitions):
        # Rotate then reverse alternating rounds to interleave all baseline arms.
        order = CASES[rep % len(CASES):] + CASES[:rep % len(CASES)]
        if rep % 2:
            order.reverse()
        for case in order:
            iterations = args.header_iterations if case.startswith('header-') else args.body_iterations
            path = out / f'rep{rep}-{case}.json'
            argv = ['taskset','-c',str(args.cpu),str(binary),case,str(iterations)]
            with path.open('w') as handle:
                process = subprocess.Popen(argv, cwd=repo, stdout=handle, stderr=subprocess.PIPE, text=True)
                _, status, usage = os.wait4(process.pid, 0)
                process.returncode = os.waitstatus_to_exitcode(status)
                stderr = process.stderr.read()
                process.stderr.close()
            commands.append({'repetition':rep,'case':case,'argv':argv,'cwd':str(repo),'exit_code':process.returncode})
            if process.returncode:
                raise SystemExit(f'probe {case} failed: {stderr}')
            result = json.loads(path.read_text())
            assert result['case'] == case and result['iterations'] == iterations
            result['repetition'] = rep
            result['process_cpu_user_seconds'] = usage.ru_utime
            result['process_cpu_system_seconds'] = usage.ru_stime
            result['process_cpu_ns_per_operation'] = (usage.ru_utime + usage.ru_stime) * 1e9 / iterations
            result['process_cpu_scope'] = 'Entire process including fixture/setup, twenty warmups, one census and output; normalized by steady-loop iterations. Not instructions.'
            result['process_peak_rss_kib'] = usage.ru_maxrss
            path.write_text(json.dumps(result, indent=2) + '\n')
            records.append(result)
            print(f'completed repetition{rep} {case}', flush=True)
    stats = {}
    for case in CASES:
        samples = [r for r in records if r['case'] == case]
        assert len(samples) == args.repetitions
        stats[case] = {
            'cpu_ns_per_operation_samples':[r['process_cpu_ns_per_operation'] for r in samples],
            'cpu_ns_per_operation_median':statistics.median(r['process_cpu_ns_per_operation'] for r in samples),
            'wall_ns_per_operation_samples':[r['loop_wall_ns_per_operation'] for r in samples],
            'wall_ns_per_operation_median':statistics.median(r['loop_wall_ns_per_operation'] for r in samples),
            'steady_allocations_per_operation': sorted(set(r['steady_allocations_per_operation'] for r in samples)),
            'steady_allocated_bytes_per_operation':sorted(set(r['steady_allocated_bytes_per_operation'] for r in samples)),
            'encoded_bytes':sorted(set(r['encoded_bytes'] for r in samples)),
            'encoded_sample_sha256':sorted(set(r['encoded_sample_sha256'] for r in samples)),
        }
    shares = {}
    for version in ['classic','flexible']:
        header = stats[f'header-{version}']['cpu_ns_per_operation_samples']
        frame = stats[f'frame-{version}']['cpu_ns_per_operation_samples']
        paired = [100*h/f for h,f in zip(header,frame)]
        rng = random.Random(53002)
        medians = [statistics.median(rng.choices(paired,k=len(paired))) for _ in range(20000)]
        shares[version] = {
            'metric':'header-only process CPU ns/op divided by full framed request process CPU ns/op',
            'paired_share_pct_samples':paired,
            'paired_median_share_pct':statistics.median(paired),
            'paired_median_share_ci95_pct':[quantile(medians,.025),quantile(medians,.975)],
            'ratio_of_medians_share_pct':100*statistics.median(header)/statistics.median(frame),
            'interpretation':'Visibility estimate, not a candidate speedup. The numerator also includes dispatch/clear/black-box overhead. Process startup/setup are amortized, not attributed as instructions.'
        }
    result = {
        'schema_version':1,'baseline_sha':source_sha,'candidate_sha':None,'all_arms_execute_baseline':True,
        'binary_sha256':sha(binary),'source_sha256':sha(source),
        'toolchain':command(['rustc','--version']).strip(),'cpu_affinity':args.cpu,
        'repetitions':args.repetitions,'processes':len(records),'header_iterations':args.header_iterations,'body_iterations':args.body_iterations,
        'order':'Rotate the five cases by round index, reverse odd rounds; no candidate A/B comparison.',
        'bootstrap':{'resamples':20000,'seed':53002,'method':'percentile CI of median matched-round header/full-request CPU-cost share'},
        'cases':stats,'header_cost_share':shares,
        'instructions':{'measured':False,'reason':'Valgrind/perf absent; configured apt metadata fetches denied by session proxy. CPU/rusage observations are not instruction counts.'},
        'limits':[
            'Baseline-only local unsigned cost study; no implementation candidate and no candidate A/B acceptance claim.',
            'Existing micro-request/v9 encodes only a100-record Produce body and is retained as a separate unchanged shape; framed probe additionally encodes the actual request header.',
            'Uses default13-byte client_id partitionline, Produce classicv8/flexiblev9,100seeded records, retained16KiB output buffer; other client-id lengths and tiny metadata/auth body shapes are not measured.',
            'Process CPU includes fixture/setup and warmup, amortized across large loops; loop wall values are observations. Managed shared host has no frequency pinning.',
            'The original named fresh-buffer body allocation census is separately saved in each micro-request-v9-body raw result; all timed probe cases use retained warmed buffers.'
        ]
    }
    (out/'cost-study.json').write_text(json.dumps(result, indent=2)+'\n')
    (out/'commands.json').write_text(json.dumps(commands, indent=2)+'\n')
    files = [p for p in sorted(out.iterdir()) if p.is_file() and p.name != 'checksums.sha256']
    (out/'checksums.sha256').write_text(''.join(sha(p)+'  '+p.name+'\n' for p in files))
    print(json.dumps(shares, indent=2))

if __name__ == '__main__':
    main()

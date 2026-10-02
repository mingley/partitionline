#!/usr/bin/env python3
"""Baseline Callgrind cost visibility for the header-cost-probe encode loop."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import random
import re
import statistics
import subprocess

CASES = ['header-classic', 'header-flexible', 'micro-request-v9-body', 'frame-classic', 'frame-flexible']

def sha(data):
    return hashlib.sha256(data).hexdigest()

def quantile(values,p):
    values=sorted(values);pos=(len(values)-1)*p;lo=int(pos);hi=min(lo+1,len(values)-1)
    return values[lo]*(1-(pos-lo))+values[hi]*(pos-lo)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root',type=Path,required=True)
    parser.add_argument('--binary',type=Path,required=True)
    parser.add_argument('--valgrind',type=Path,required=True)
    parser.add_argument('--valgrind-lib',type=Path,required=True)
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--cpu',type=int,required=True)
    parser.add_argument('--repetitions',type=int,default=5)
    parser.add_argument('--header-iterations',type=int,default=10_000)
    parser.add_argument('--body-iterations',type=int,default=100)
    args=parser.parse_args()
    if min(args.repetitions,args.header_iterations,args.body_iterations)<1:
        parser.error('counts must be positive')
    repo=args.repo_root.resolve();out=args.out.resolve();out.mkdir(parents=True,exist_ok=True)
    binary=args.binary.resolve();valgrind=args.valgrind.resolve();library=args.valgrind_lib.resolve()
    env=os.environ.copy();env['VALGRIND_LIB']=str(library)
    baseline=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
    if subprocess.check_output(['git','status','--porcelain'],cwd=repo,text=True).strip():
        raise SystemExit('baseline checkout is dirty')
    tool_version=subprocess.check_output([str(valgrind),'--version'],env=env,text=True).strip()
    commands=[];samples=[]
    for rep in range(args.repetitions):
        order=CASES[rep%len(CASES):]+CASES[:rep%len(CASES)]
        if rep%2: order.reverse()
        for case in order:
            iterations=args.header_iterations if case.startswith('header-') else args.body_iterations
            prefix=out/f'rep{rep}-{case}';raw=prefix.with_suffix('.callgrind.out')
            argv=['taskset','-c',str(args.cpu),str(valgrind),'--tool=callgrind','--collect-atstart=no','--toggle-collect=header_cost_probe::encode_case',f'--callgrind-out-file={raw}',str(binary),case,str(iterations)]
            run=subprocess.run(argv,cwd=repo,env=env,text=True,capture_output=True)
            commands.append({'repetition':rep,'case':case,'argv':argv,'cwd':str(repo),'VALGRIND_LIB':str(library),'exit_code':run.returncode})
            prefix.with_suffix('.stdout.json').write_text(run.stdout)
            prefix.with_suffix('.stderr.txt').write_text(run.stderr)
            if run.returncode:
                (out/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
                raise SystemExit(f'Callgrind failed for {case} with exit{run.returncode}')
            data=raw.read_bytes();text=data.decode()
            if re.search(r'^events:\s*Ir\s*$',text,re.M) is None:
                raise SystemExit('expected exactly Ir events')
            totals=re.findall(r'^summary:\s*(\d+)\s*$',text,re.M)
            if len(totals)!=1 or int(totals[0])==0:
                raise SystemExit('missing/non-positive Callgrind Ir summary; function selector may have failed')
            output=json.loads(run.stdout);assert output['case']==case and output['iterations']==iterations
            # encode_case is called twenty times for warmup, once for census,
            # then N loop calls. All are collected; setup outside it is not.
            calls=iterations+output['warmup_iterations']+1
            sample={'repetition':rep,'case':case,'iterations':iterations,'collected_calls':calls,'instructions_retired':int(totals[0]),'instructions_per_encode_case':int(totals[0])/calls,'raw_callgrind_sha256':sha(data),'encoded_sample_sha256':output['encoded_sample_sha256'],'encoded_bytes':output['encoded_bytes'],'steady_allocations_per_operation':output['steady_allocations_per_operation'],'steady_allocated_bytes_per_operation':output['steady_allocated_bytes_per_operation'],'header_decode_checks_passed':output['header_decode_checks_passed']}
            packed=gzip.compress(data,mtime=0);prefix.with_suffix('.callgrind.out.gz').write_bytes(packed);raw.unlink()
            sample['compressed_callgrind_sha256']=sha(packed)
            prefix.with_suffix('.sample.json').write_text(json.dumps(sample,indent=2)+'\n');samples.append(sample)
            print(f'completed repetition{rep} {case}: Ir/call={sample["instructions_per_encode_case"]:.4f}',flush=True)
    stats={}
    for case in CASES:
        values=[s['instructions_per_encode_case'] for s in samples if s['case']==case]
        stats[case]={'instructions_per_operation_samples':values,'instructions_per_operation_median':statistics.median(values)}
    shares={}
    for version in ['classic','flexible']:
        h=stats['header-'+version]['instructions_per_operation_samples'];f=stats['frame-'+version]['instructions_per_operation_samples']
        paired=[100*a/b for a,b in zip(h,f)];rng=random.Random(53003)
        bootstrap=[statistics.median(rng.choices(paired,k=len(paired))) for _ in range(20000)]
        shares[version]={'paired_share_pct_samples':paired,'paired_median_share_pct':statistics.median(paired),'paired_median_share_ci95_pct':[quantile(bootstrap,.025),quantile(bootstrap,.975)],'ratio_of_medians_share_pct':100*statistics.median(h)/statistics.median(f)}
    result={'schema_version':1,'baseline_sha':baseline,'candidate_sha':None,'all_arms_execute_baseline':True,'binary_sha256':sha(binary.read_bytes()),'source_sha256':sha(Path(__file__).with_name('header-cost-probe.rs').read_bytes()),'tool':'Callgrind','tool_version':tool_version,'cpu_affinity':args.cpu,'repetitions':args.repetitions,'processes':len(samples),'order':'Rotate five cases by round index, reverse odd rounds; baseline-only, not candidate A/B','collection_scope':'Only inclusive header_cost_probe::encode_case calls (twenty warmups, one allocation census, N timed-loop calls); process/fixture/setup/output outside this function excluded. Divisor=N+21. No kernel instructions counted.','cases':stats,'header_instruction_share':shares,'bootstrap':{'resamples':20000,'seed':53003,'method':'percentile CI of median matched-round header/full-frame instruction-cost share'},'limits':['Default13-byte client_id; Producev8 classic/v9 flexible;100seeded records;retained16KiB output buffer. No tiny RPC bodies or long client IDs measured.','Header numerator includes dispatch/clear/black-box overhead and all header fields, including correlation ID; preencoding cannot eliminate all of it.','This does not change the original body-only micro-request benchmark, ratchet an instruction baseline, or demonstrate a candidate speedup.','Probe uses the existing codec CountingAlloc allocator; timed allocation counts are separately recorded. Instruction collection includes one census-enabled call and all other census-disabled calls.']}
    (out/'instruction-study.json').write_text(json.dumps(result,indent=2)+'\n');(out/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
    files=[p for p in sorted(out.iterdir()) if p.is_file() and p.name!='checksums.sha256'];(out/'checksums.sha256').write_text(''.join(sha(p.read_bytes())+'  '+p.name+'\n' for p in files))
    print(json.dumps(shares,indent=2))

if __name__=='__main__':main()

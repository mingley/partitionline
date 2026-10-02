#!/usr/bin/env python3
"""Validate the frozen baseline-only CPU and Callgrind visibility artifacts."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import random
import re
import statistics

CASES=['header-classic','header-flexible','micro-request-v9-body','frame-classic','frame-flexible']

def quantile(values,p):
    values=sorted(values);pos=(len(values)-1)*p;lo=int(pos);hi=min(lo+1,len(values)-1)
    return values[lo]*(1-(pos-lo))+values[hi]*(pos-lo)

def sha(data):return hashlib.sha256(data).hexdigest()

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifacts',type=Path,default=Path(__file__).resolve().parent)
    parser.add_argument('--output',type=Path)
    args=parser.parse_args();root=args.artifacts.resolve()
    cpu=json.loads((root/'cpu/cost-study.json').read_text());ir=json.loads((root/'instructions/instruction-study.json').read_text())
    assert cpu['baseline_sha']==ir['baseline_sha']=='aeb205ddee136d915349ef9dd165d58e9d3aa109'
    assert cpu['candidate_sha'] is None and ir['candidate_sha'] is None
    assert cpu['all_arms_execute_baseline'] and ir['all_arms_execute_baseline']
    assert cpu['binary_sha256']==ir['binary_sha256']
    source=sha((root/'header-cost-probe.rs').read_bytes())
    assert source==cpu['source_sha256']==ir['source_sha256']
    assert ir['tool_version']=='valgrind-3.24.0'
    assert cpu['repetitions']==ir['repetitions']==5 and cpu['processes']==ir['processes']==25
    for family,summary in [('cpu',cpu),('instructions',ir)]:
        for line in (root/family/'checksums.sha256').read_text().splitlines():
            digest,name=line.split('  ',1);assert sha((root/family/name).read_bytes())==digest
    samples={'cpu':{},'instructions':{}}
    for rep in range(5):
        for case in CASES:
            c=json.loads((root/'cpu'/f'rep{rep}-{case}.json').read_text())
            i=json.loads((root/'instructions'/f'rep{rep}-{case}.sample.json').read_text())
            stdout=json.loads((root/'instructions'/f'rep{rep}-{case}.stdout.json').read_text())
            assert c['case']==i['case']==stdout['case']==case and c['repetition']==i['repetition']==rep
            assert c['candidate'] is False and stdout['candidate'] is False
            assert c['encoded_sample_sha256']==i['encoded_sample_sha256']==stdout['encoded_sample_sha256']
            assert c['encoded_bytes']==i['encoded_bytes']==stdout['encoded_bytes']
            assert c['steady_allocations_per_operation']==i['steady_allocations_per_operation']==stdout['steady_allocations_per_operation']
            assert c['steady_allocated_bytes_per_operation']==i['steady_allocated_bytes_per_operation']==stdout['steady_allocated_bytes_per_operation']
            if case.startswith('header-'):
                assert c['steady_allocations_per_operation']==c['steady_allocated_bytes_per_operation']==0
                assert c['header_decode_checks_passed'] and stdout['header_decode_checks_passed']
                assert c['encoded_bytes']==(23 if case.endswith('classic') else 24)
            if case.startswith('frame-'):
                assert c['header_decode_checks_passed'] and stdout['header_decode_checks_passed']
            if case=='micro-request-v9-body':
                assert c['original_named_body_allocations']==stdout['original_named_body_allocations']==17
                assert c['original_named_body_allocated_bytes']==stdout['original_named_body_allocated_bytes']==70766
            packed=(root/'instructions'/f'rep{rep}-{case}.callgrind.out.gz').read_bytes();raw=gzip.decompress(packed)
            assert sha(packed)==i['compressed_callgrind_sha256'] and sha(raw)==i['raw_callgrind_sha256']
            assert re.search(rb'^events:\s*Ir\s*$',raw,re.M)
            totals=re.findall(rb'^summary:\s*(\d+)\s*$',raw,re.M)
            assert len(totals)==1 and int(totals[0])==i['instructions_retired']>0
            assert i['collected_calls']==i['iterations']+21
            assert i['instructions_per_encode_case']==i['instructions_retired']/i['collected_calls']
            samples['cpu'].setdefault(case,[]).append(c['process_cpu_ns_per_operation'])
            samples['instructions'].setdefault(case,[]).append(i['instructions_per_encode_case'])
    for family,summary,key,seed in [('cpu',cpu,'header_cost_share',53002),('instructions',ir,'header_instruction_share',53003)]:
        for variant in ['classic','flexible']:
            header=samples[family]['header-'+variant];frame=samples[family]['frame-'+variant]
            paired=[100*h/f for h,f in zip(header,frame)];rng=random.Random(seed)
            bootstrap=[statistics.median(rng.choices(paired,k=5)) for _ in range(20000)]
            expected=summary[key][variant]
            assert expected['paired_share_pct_samples']==paired
            assert expected['paired_median_share_pct']==statistics.median(paired)
            assert expected['paired_median_share_ci95_pct']==[quantile(bootstrap,.025),quantile(bootstrap,.975)]
            assert max(expected['paired_median_share_ci95_pct'])<1
    for family in ['cpu','instructions']:
        commands=json.loads((root/family/'commands.json').read_text())
        measured=[c for c in commands if 'repetition' in c]
        assert len(measured)==25 and all(c['exit_code']==0 for c in measured)
    provenance=json.loads((root/'tooling/verification.json').read_text())
    metadata=(root/'tooling/valgrind.metadata.txt').read_text()
    expected=next(line[8:] for line in metadata.splitlines() if line.startswith('SHA256: '))
    assert provenance['sha256']==provenance['expected_metadata_sha256']==expected
    result={'valid':True,'errors':0,'baseline_sha':cpu['baseline_sha'],'candidate_sha':None,'cpu_processes_validated':25,'instruction_processes_validated':25,'compressed_callgrind_checksums_verified':25,'decompressed_callgrind_checksums_verified':25,'original_named_body_census':{'allocations':17,'bytes':70766,'samples':10},'header_zero_allocation_samples':20,'confidence_intervals_reproduced':4,'tool_package_metadata_checksum_matches':True,'source_and_binary_hashes_agree':True,'standard_runtime_contract_validator_used':False,'scope':'Read-only evidence-local visibility probes, not runtime result-contract artifacts or candidate A/B acceptance evidence.'}
    text=json.dumps(result,indent=2)+'\n'
    if args.output:args.output.write_text(text)
    print(text,end='')

if __name__=='__main__':main()

#!/usr/bin/env python3
"""Separate measured client phase samples from fixture/setup samples."""
import argparse
import collections
import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess


def nanoseconds(iso):
    match=re.fullmatch(r'(.*T\d\d:\d\d:\d\d)(?:\.(\d+))?Z',iso)
    if not match:raise ValueError('UTC timestamp required')
    base=datetime.datetime.fromisoformat(match[1]+'+00:00')
    return int(base.timestamp())*1_000_000_000+int((match[2] or '').ljust(9,'0'))


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--profile',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();b=a.profile
    intervals=[]
    for row in json.loads((b/'rows.json').read_text()):
        intervals.append((nanoseconds(row['phase']['start_time_utc']),nanoseconds(row['phase']['end_time_utc'])))
    intervals.sort()
    for i,(start,end) in enumerate(intervals):
        if start>=end or i and start<intervals[i-1][1]:raise ValueError('overlapping or invalid phase bounds')
    text=(b/'stacks.stdout').read_text();samples=[];symbols=set()
    for block in text.strip().split('\n\n'):
        lines=block.splitlines()
        match=re.fullmatch(r'runtime\s+(\d+)\s+(\d+)\.(\d+):\s+(\d+)\s+cpu-clock:u:\s*',lines[0])
        if not match:raise ValueError('unexpected actual CPU sample header: '+lines[0])
        stamp=int(match[2])*1_000_000_000+int(match[3].ljust(9,'0'));period=int(match[4]);frames=[]
        for line in lines[1:]:
            frame=re.fullmatch(r'\s*([0-9a-f]+)\s+(.+)\s+\((.+)\)\s*',line)
            if not frame:raise ValueError('unexpected actual stack frame')
            name=frame[2].rsplit('+0x',1)[0];symbols.add(name);frames.append((name,frame[3],frame[1]))
        if not frames or period<=0:raise ValueError('no valid sampled frames/period')
        samples.append(dict(pid=int(match[1]),timestamp_ns=stamp,period=period,frames=frames,
                            measured=any(start<=stamp<=end for start,end in intervals)))
    ordered=sorted(symbols);demangled=subprocess.run(['c++filt','-s','rust'],input='\n'.join(ordered)+'\n',text=True,capture_output=True,check=True).stdout.splitlines()
    if len(demangled)!=len(ordered):raise ValueError('demangler changed symbol count')
    names=dict(zip(ordered,demangled));leaf=collections.Counter();inclusive=collections.Counter();total=0;count=0;unknown=0
    for row in samples:
        if not row['measured']:continue
        count+=1;total+=row['period'];first=row['frames'][0][0]
        if first.startswith('0x') or first in ('[unknown]','[UNKNOWN]'):unknown+=row['period']
        leaf[names[first]]+=row['period']
        for name in {names[frame[0]] for frame in row['frames']}:inclusive[name]+=row['period']
    if count==0 or total<=0:raise ValueError('no measured client samples')
    def rows(counter):return [dict(symbol=name,period_ns=period,share_percent=100*period/total) for name,period in counter.most_common()]
    result=dict(scope='Local sampled userspace CPU; original ELF; actual phase timestamps; no hardware cycle estimate',
        source_commit=json.loads((b/'plan.json').read_text())['source_commit'],raw_script_sha256=hashlib.sha256((b/'stacks.stdout').read_bytes()).hexdigest(),
        phase_count=len(intervals),all_runtime_samples=len(samples),measured_runtime_samples=count,
        excluded_setup_fixture_shutdown_samples=len(samples)-count,sample_period_total_ns=total,
        unresolved_leaf_percent=100*unknown/total,event='cpu-clock:u',nominal_frequency=997,
        self_symbols=rows(leaf),inclusive_symbols=rows(inclusive),
        limitations=['Symbol attribution groups inlined work into enclosing functions. A missing symbol is not a measured zero.',
                     'Inclusive symbol shares overlap and must not be summed.',
                     'User-only sampling excludes kernel CPU and off-CPU waits.',
                     'Sampling and observer overhead make these diagnostic profiles, not comparable throughput measurements.'])
    with a.output.open('x') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps({k:result[k] for k in ('measured_runtime_samples','excluded_setup_fixture_shutdown_samples','unresolved_leaf_percent')}))
    for row in result['self_symbols'][:12]:print(round(row['share_percent'],2),row['symbol'])


if __name__=='__main__':main()

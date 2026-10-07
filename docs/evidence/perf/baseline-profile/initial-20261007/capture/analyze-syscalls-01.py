#!/usr/bin/env python3
"""Count client syscalls whose entries fall inside actual measured phases."""
import argparse
import collections
import hashlib
import importlib.util
import json
from pathlib import Path
import re


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--profile',type=Path,required=True)
    p.add_argument('--cpu-analyzer',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();b=a.profile
    spec=importlib.util.spec_from_file_location('phase_clock',a.cpu_analyzer)
    clock=importlib.util.module_from_spec(spec);spec.loader.exec_module(clock)
    completion=json.loads((b/'completion.json').read_text())
    if not completion['source_guards_passed'] or not completion['owned_process_groups_empty']:
        raise ValueError('capture not completed with source/process guards')
    pid=int((b/'runtime.pid').read_text());rows=json.loads((b/'rows.json').read_text())
    intervals=[(clock.nanoseconds(r['phase']['start_time_utc']),clock.nanoseconds(r['phase']['end_time_utc'])) for r in rows]
    if any(lo>=hi or i and lo<intervals[i-1][1] for i,(lo,hi) in enumerate(intervals)):
        raise ValueError('invalid phase intervals')
    counts=[collections.Counter() for _ in rows];elapsed=[collections.Counter() for _ in rows]
    errors=[collections.Counter() for _ in rows];written=[collections.Counter() for _ in rows]
    pending=None;client_entries=0;excluded=0;descendant_lines=0
    trace=b/'syscalls.txt';digest=hashlib.sha256()
    for raw in trace.open('rb'):
        digest.update(raw);line=raw.decode().strip()
        match=re.fullmatch(r'(\d+) (\d+)\.(\d+) (.*)',line)
        if not match:raise ValueError('unexpected raw syscall prefix')
        if int(match[1])!=pid:descendant_lines+=1;continue
        stamp=int(match[2])*10**9+int(match[3].ljust(9,'0'));body=match[4]
        if body.startswith(('--- ','+++ ')):continue
        resumed=re.match(r'<\.\.\. (\w+) resumed>',body)
        if resumed:
            if pending is None or pending[0]!=resumed[1]:raise ValueError('resumed syscall has no matching entry')
            name,index=pending;pending=None
        else:
            entry=re.match(r'(\w+)\(',body)
            if not entry:raise ValueError('unexpected client syscall: '+body)
            if pending is not None:raise ValueError('new syscall before prior call resumed')
            name=entry[1];client_entries+=1
            index=next((i for i,(lo,hi) in enumerate(intervals) if lo<=stamp<hi),None)
            if index is None:excluded+=1
            else:counts[index][name]+=1
            if '<unfinished ...>' in body:
                pending=(name,index);continue
        if index is None:continue
        duration=re.search(r'<(\d+)\.(\d+)>$',body)
        if duration:elapsed[index][name]+=int(duration[1])*10**9+int(duration[2].ljust(9,'0'))
        error=re.search(r'= -1 (\w+)',body)
        if error:errors[index][name+':'+error[1]]+=1
        returned=re.search(r'= (0x[0-9a-f]+|\d+)(?:\s|$)',body)
        if name in ('write','writev','sendto','sendmsg','sendmmsg') and returned:
            # sendmmsg returns messages, not bytes; retain only byte-returning calls.
            if name!='sendmmsg':written[index][name]+=int(returned[1],0)
    if pending is not None:raise ValueError('unclosed client syscall')
    phases=[]
    for i,row in enumerate(rows):
        count=sum(counts[i].values())
        if count<=0:raise ValueError('empty measured client syscall phase')
        phases.append(dict(bounds_utc=row['phase'],entry_count=count,
            calls=[dict(syscall=name,count=n,count_share_percent=100*n/count,
                        elapsed_ns=elapsed[i][name],successful_returned_write_bytes=written[i][name])
                   for name,n in counts[i].most_common()],errors=dict(errors[i])))
    result=dict(source_commit=json.loads((b/'plan.json').read_text())['source_commit'],
        runtime_main_pid=pid,trace_sha256=digest.hexdigest(),analyzer_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        phase_clock_analyzer_sha256=hashlib.sha256(a.cpu_analyzer.read_bytes()).hexdigest(),
        client_lifetime_entries=client_entries,excluded_client_entries=excluded,excluded_descendant_lines=descendant_lines,
        phases=phases,scope='Actual runtime main-thread entries within measured UTC phases; no broker or RSS-sampler thread counts',
        limitations=['Syscall elapsed time includes blocking and ptrace overhead; it is not CPU time.',
            'Raw arguments expose no strings, payloads, I/O-vector elements or TLS record sizes.',
            'Byte-returning calls report actual returned bytes; file/socket descriptors are not classified.',
            'Count share does not measure syscall CPU cost. VDSO clock reads do not appear as syscalls.'])
    with a.output.open('x') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(dict(measured_entries=sum(p['entry_count'] for p in phases),excluded_client_entries=excluded,phases=len(phases))))


if __name__=='__main__':main()

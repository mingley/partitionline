#!/usr/bin/env python3
"""Five actual process ownership controls; zero broker/OIDC/KafkaSDK runs."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile


HELPER = r'''
import json,os,signal,subprocess,sys,time
from pathlib import Path
def ident(pid):
    f=(Path('/proc')/str(pid)/'stat').read_text().rsplit(')',1)[1].split()
    return {'pid':pid,'pgid':int(f[2]),'starttime_ticks':int(f[19])}
registry,mode,ready=map(str,sys.argv[1:])
rows=[]
if mode in ('orphan','ignore-term','missing-registry'):
    child="import os,signal,sys,time\nfrom pathlib import Path\n"
    if mode=='ignore-term': child+="signal.signal(signal.SIGTERM,signal.SIG_IGN)\n"
    child+="Path(sys.argv[1]).write_text('ready')\ntime.sleep(60)\n"
    process=subprocess.Popen([sys.executable,'-c',child,ready],start_new_session=True)
    deadline=time.monotonic()+3
    while not Path(ready).exists() and time.monotonic()<deadline:time.sleep(.01)
    assert Path(ready).exists()
    rows=[{'actor':'actual-synthetic-grandchild',**ident(process.pid)}]
if mode!='missing-registry':
    Path(registry).write_text(json.dumps({'driver':ident(os.getpid()),'owned_groups':rows}))
if mode=='graceful': time.sleep(.2)
elif mode=='timeout':
    signal.signal(signal.SIGTERM,lambda *_:sys.exit(0))
    time.sleep(60)
'''


def require(condition,label):
    if not condition:
        raise RuntimeError(label)


def main():
    sys.dont_write_bytecode=True
    source=Path(__file__).resolve().parent.parent/'own-live.py'
    spec=importlib.util.spec_from_file_location('outer_owner',source)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    results=[]
    with tempfile.TemporaryDirectory(prefix='oidc-outer-owner-control-',dir='/workspace/work') as directory:
        base=Path(directory)
        for mode in ('graceful','orphan','ignore-term','missing-registry','timeout'):
            registry=base/(mode+'.registry.json')
            owner=module.Owner(registry,budget=.5 if mode=='timeout' else 5,grace=.2,term_grace=.2)
            result=owner.run([sys.executable,'-c',HELPER,str(registry),mode,str(base/(mode+'.ready'))])
            require(result['passed']==(mode=='graceful'),'predeclared process ownership outcome')
            require(not result['remaining'] and not result['unconfirmed_registered_group_members'],
                    'no surviving controlled processes or adopted zombies')
            if mode=='ignore-term':
                require(any(event.get('signal')==9 for event in result['events']),
                        'TERM-ignoring actual grandchild requires KILL')
            if mode=='missing-registry':
                require(result['unverified_ownership'] and result['forced_cleanup'],
                        'missing registry race swept but cannot pass ownership')
            if mode=='timeout': require(result['timed_out'] and result['forced_cleanup'],'absolute budget cannot pass')
            results.append({'case':mode,'assertions_passed':True,'actual_owner_result':result})
    receipt={'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
             'scope':'5 actual Linux subreaper/proc/new-session process controls only; zero OIDC/broker/SDK/Cargo qualification',
             'passed':True,'cases':results}
    path=Path(__file__).with_name('outer-controls.json')
    path.write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps({'controls':len(results),'passed':True,
                      'receipt_sha256':hashlib.sha256(path.read_bytes()).hexdigest()}))


if __name__=='__main__':main()

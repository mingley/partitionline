#!/usr/bin/env python3
import importlib.util, json, os, shutil
from pathlib import Path
spec=importlib.util.spec_from_file_location('broker_runner',Path(__file__).with_name('run.py'));r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
out=Path('/workspace/work/goal-qualification-20261028/broker-clean-doctests-01')
expected='de38cd3df49daa73474156643de7fae294882cdb'
if out.exists():raise SystemExit('output exists')
if r.git('rev-parse','HEAD')!=expected or r.git('status','--porcelain=v1','--untracked-files=all') or r.ignored_inputs():raise SystemExit('unexpected HEAD or dirty inputs')
if shutil.disk_usage(out.parent).free < 3*1024**3:raise SystemExit('less than 3 GiB free')
out.mkdir()
env=dict(os.environ,RUSTUP_HOME='/workspace/work/rustup',CARGO_HOME='/workspace/work/cargo',PATH='/workspace/work/cargo/bin:'+os.environ['PATH'],CARGO_TARGET_DIR=r.TARGET,CARGO_BUILD_JOBS='2',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
for key in ('RUST_TEST_THREADS','RUST_MIN_STACK','RUSTFLAGS','RUSTDOCFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER'):env.pop(key,None)
baseline=r.input_hashes(); results=[]
for label,args in [('doc-default',['test','--locked','--manifest-path',r.BROKER,'--doc']),('doc-all-features',['test','--locked','--manifest-path',r.BROKER,'--doc','--all-features'])]:
 if shutil.disk_usage(out).free<3*1024**3: print('STOP before',label,'start disk floor',flush=True);break
 item=r.run_case(label,args,out,env,expected,baseline,2400,2*1024**3)
 results.append({key:item[key] for key in ('command','exit','stop_reason','inputs_stable','oom_kills_increased','results')})
 if item['stop_reason'] or not item['inputs_stable'] or item['oom_kills_increased']:break
summary={'head':expected,'completed_cases':results,'complete':len(results)==2,'all_commands_succeeded':len(results)==2 and all(x['exit']==0 and not x['stop_reason'] and x['inputs_stable'] and not x['oom_kills_increased'] for x in results),'note':'Separate follow-up; original broker-clean-matrix-01 remains incomplete.'}
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
raise SystemExit(0 if summary['all_commands_succeeded'] else 1)

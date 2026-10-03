#!/usr/bin/env python3
"""Future immutable full-source focused run; requires a separate root lease and pin."""
import argparse,gzip,hashlib,io,json,os,re,signal,subprocess,tarfile,time
from pathlib import Path
P=argparse.ArgumentParser()
for name in ('source','tree','origin','scratch','plan'):P.add_argument('--'+name,required=True)
P.add_argument('--stage',choices=('image-three','image-five','owner-controls'),required=True)
P.add_argument('--clean-package',action='store_true')
P.add_argument('--prior-cache-validation')
P.add_argument('--prior-cache-sha256')
P.add_argument('--platform-daemon-pin')
P.add_argument('--platform-daemon-sha256')
A=P.parse_args();driver=Path(__file__).resolve();REPO=Path('/workspace/partitionline');TREE=Path(A.tree).resolve();OUT=Path(A.scratch).resolve();CACHE=Path('/workspace/work/target-broker-segments')
assert not OUT.exists();OUT.mkdir(parents=True)
DISK_FLOOR_BYTES=350*1024*1024;DISK_POLL_SECONDS=0.2
ENV=dict(os.environ,CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',PATH='/workspace/work/cargo/bin:'+os.environ['PATH'],CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_TARGET_DIR=str(CACHE),CARGO_NET_OFFLINE='true')
os.sched_setaffinity(0,{2,4})
assert os.sched_getaffinity(0)=={2,4}
for q in (REPO,TREE,OUT):assert not q.is_relative_to(CACHE) and not CACHE.is_relative_to(q)
prov=json.loads(driver.with_name('guard-provenance.json').read_text());helpers=driver.with_name('guard-functions.py');cache_helpers=driver.with_name('cache-guards.py')
assert hashlib.sha256(helpers.read_bytes()).hexdigest()==prov['extracted_sha256']
assert hashlib.sha256(cache_helpers.read_bytes()).hexdigest()==prov['cache_functions_sha256']
assert hashlib.sha256(driver.with_name('artifact_paths.py').read_bytes()).hexdigest()==prov['artifact_parser_sha256']
assert hashlib.sha256(driver.with_name('process_guards.py').read_bytes()).hexdigest()==prov['process_guard_sha256']
assert bool(A.platform_daemon_pin)==bool(A.platform_daemon_sha256), 'exact optional daemon pin requires both path and SHA; default is no exception'
elf_objects={};elf_snapshots={}
receipt={'environment':{k:ENV[k] for k in ('CARGO_HOME','RUSTUP_HOME','CARGO_TARGET_DIR','CARGO_INCREMENTAL','CARGO_BUILD_JOBS','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','CARGO_NET_OFFLINE')},'schema_version':1,'task':'KL11-76','scope':'focused genuine runtime behavior/ownership on complete immutable source; not whole-broker final qualification','source_commit':A.source,'source_tree':str(TREE),'driver_sha256':hashlib.sha256(driver.read_bytes()).hexdigest(),'guard_provenance':prov,'commands':[],'stage':A.stage,'affinity':[2,4]}
exec(compile(helpers.read_text(),str(helpers),'exec'),globals())
exec(compile(cache_helpers.read_text(),str(cache_helpers),'exec'),globals())
master_path=Path(A.plan);master=json.loads(master_path.read_text());plan=master['stages'][A.stage];assert master['no_execution_authorization_in_plan']
receipt['plan']={'path':str(master_path),'sha256':sha256_file(master_path)}
def save():
    guard_write();receipt['retained_elf_objects']=list(elf_objects.values());(OUT/'validation.json').write_text(json.dumps(receipt,indent=2)+'\n')
def load_origin(path,source,root):
    origin=json.loads(path.read_bytes());assert origin['source_commit']==source and Path(origin['source_directory']).resolve()==root
    m=origin['source_manifest'];mp=Path(m['path']);assert sha256_file(mp)==m['compressed_sha256'];raw=gzip.decompress(mp.read_bytes());assert hashlib.sha256(raw).hexdigest()==m['uncompressed_sha256'];pins=json.loads(raw)
    actual={}
    for row in subprocess.check_output(['git','ls-tree','-r','-z',source],cwd=REPO).split(b'\0'):
        if row:
            meta,name=row.split(b'\t',1);mode,kind,blob=meta.decode().split();assert kind=='blob';actual[name.decode()]={'mode':mode,'blob':blob}
    assert set(actual)==set(pins)
    for n,p in pins.items():assert actual[n]=={'mode':p['mode'],'blob':p['git_blob_sha1']}
    return pins,{'path':str(path),'sha256':sha256_file(path),'manifest':m,'full_git_path_set_verified':True}
expected,receipt['source_origin']=load_origin(Path(A.origin),A.source,TREE)
BASE=Path(master['original_basis']['tree']);baseline,receipt['original_origin']=load_origin(Path(master['original_basis']['origin']),master['original_basis']['commit'],BASE)
assert len(baseline)==73171
for r in master['candidate_sources']:
    p=TREE/r['path'];assert sha256_file(p)==r['sha256'] and (p.stat().st_mode&0o7777)==expected[r['path']]['full_permission_mode']
def guards():return {'complete_current':verify_rows(TREE,expected),'original_ea9_full73171':verify_rows(BASE,baseline)}
prior_validation_pin=dict(master['prior_cache_retention'])
if A.prior_cache_validation:
    assert A.prior_cache_sha256
    prior_validation_pin={'validation':A.prior_cache_validation,'sha256':A.prior_cache_sha256}
else:assert not A.prior_cache_sha256
vp=Path(prior_validation_pin['validation']);assert sha256_file(vp)==prior_validation_pin['sha256'];prior=json.loads(vp.read_bytes());reference_cache_receipt=prior['whole_cache_retention'][-1]
reference_cache_map_path=Path(reference_cache_receipt['map']);assert sha256_file(reference_cache_map_path)==reference_cache_receipt['map_sha256'];reference_cache_map=json.loads(reference_cache_map_path.read_bytes())
for obj in prior['retained_elf_objects']:
    obj=dict(obj);p=Path(obj['path']);p=p if p.is_absolute() else vp.parent/p;assert sha256_file(p)==obj['sha256'];obj['path']=str(p);elf_objects[obj['uncompressed_sha256']]=obj
for row in prior['all_current_elfs']:
    p=Path(row['original_path']);assert sha256_file(p)==row['sha256'] and (p.stat().st_mode&0o7777)==row['original_mode'];elf_snapshots[str(p)]={'identity':elf_identity(p.stat()),'sha256':row['sha256']}
receipt['inherited_cache_receipt']=prior_validation_pin
# Only owned Cargo package cleanup is permitted, after full original bytes,
# non-ELF outputs, current ELFs and archive composition have been verified.
def signal_stop(n,_):raise KeyboardInterrupt('owned runner signal '+str(n))
signal.signal(signal.SIGTERM,signal_stop)
schedule=[]
if A.clean_package:schedule.append(('package-clean',['taskset','-c','2,4','cargo','+stable','clean','--offline','--locked','--manifest-path','partitionline-broker/Cargo.toml','--target-dir',str(CACHE),'--package','partitionline-broker']))
schedule.append((A.stage,plan['argv']))
try:
    receipt['toolchain']=subprocess.check_output(['rustc','+stable','-Vv'],env=ENV,text=True);receipt['initial_sources']=guards();assert not cache_owners();receipt['initial_forecast']=combined_forecast();assert receipt['initial_forecast']['sufficient'];save()
    tag=CACHE/'CACHEDIR.TAG';assert tag.stat().st_size==177 and sha256_file(tag)=='6d9d1d216e0f83abc5e5662ca62c92b4f23009466b54fa27321a69acdb778bb2'
    # Reuse prior complete-cache/delta archives instead of duplicating them.
    preserve_whole_cache('pre-clean');retain_cache('pre-clean','all current ELF verification before any overwrite',force_verify=True)
    for name,argv in schedule:
        before=guards();owners=cache_owners();assert not owners
        forecast=combined_forecast();assert forecast['sufficient'];receipt.setdefault('pre_command_forecasts',[]).append({'name':name,**forecast});save()
        lane=OUT/name;lane.mkdir();capture=OUT/'captures'/name;capture.mkdir(parents=True)
        temporary=OUT/'runtime-temporary'/name;temporary.mkdir(parents=True)
        ENV.update(PL_PEER_RUNTIME_CAPTURE_DIR=str(capture),PL_PEER_RUNTIME_SOURCE_SHA=A.source,TMPDIR=str(temporary))
        with (lane/'command.log').open('w') as log:code,monitor=monitored_command(argv,TREE,ENV,log,lane/'disk-monitor.jsonl')
        (lane/'command.exit').write_text(str(code)+'\n');output=(lane/'command.log').read_text()
        item={'name':name,'argv':argv,'exit_code':code,'source_before':before,'disk_monitor':monitor,'log_sha256':sha256_file(lane/'command.log'),'environment_additions':{'PL_PEER_RUNTIME_CAPTURE_DIR':str(capture),'PL_PEER_RUNTIME_SOURCE_SHA':A.source}}
        receipt['commands'].append(item);save()
        # Preserve all generated objects before source/harness qualification can
        # refuse. A parser failure must never bypass whole-cache/ELF retention.
        item['post_command_elfs']=retain_cache(name,'all success/failure current ELFs BEFORE harness parsing',force_verify=True);receipt['all_current_elfs']=item['post_command_elfs'];save()
        retained=preserve_whole_cache('post-'+name);reference_cache_receipt=retained;reference_cache_map=json.loads(Path(retained['map']).read_bytes());save()
        item['captures']={str(p.relative_to(capture)):{'sha256':sha256_file(p),'bytes':p.stat().st_size,'full07777':p.stat().st_mode&0o7777}for p in sorted(capture.rglob('*'))if p.is_file()}
        item['temporary_files']={str(p.relative_to(temporary)):{'sha256':sha256_file(p),'bytes':p.stat().st_size,'full07777':p.stat().st_mode&0o7777}for p in sorted(temporary.rglob('*'))if p.is_file()}
        receipt['actual_capture_bytes']=sum(r['bytes'] for r in item['captures'].values());item['post_command_preservation_completed_before_harness_parsing']=True;save()
        item['source_after']=guards();item['source_after_retention']=item['source_after'];save()
        item['executed_elfs']=executed_elfs(output,name);save()
        if code or monitor['triggered']:raise AssertionError('first actual compile/test/disk failure retained; no further commands')
        if name=='package-clean':
            assert any(n.endswith('.rlib') and 'partitionline_broker' in n for n in retained['deleted_prior_paths']), 'own-package fresh compilation not established'
            continue
        summaries=re.findall(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;',output)
        assert len(summaries)==1;outcome,passed,failed,ignored,measured,filtered=summaries[0]
        item['actual_tests']={'passed':int(passed),'failed':int(failed),'ignored':int(ignored),'measured':int(measured),'filtered':int(filtered)}
        assert outcome=='ok' and int(passed)==plan['expected_selected_tests'] and int(failed)==int(ignored)==int(measured)==0 and item['executed_elfs']
        if A.clean_package:
            assert '--crate-name partitionline_broker' in output and '--test' in output
            assert str(TREE/'partitionline-broker') in output
            item['fresh_rustc_package_binding']=True
        assert receipt['actual_capture_bytes']<=plan['raw_capture_bytes_upper_bound'];save()
    receipt['final_sources']=guards();receipt['final_cache_owners']=cache_owners();assert not receipt['final_cache_owners'];receipt['runner_outcome']='focused stage passed with source/cache/capture preservation; independent causal review and remaining qualification pending';save()
except BaseException as e:
    receipt['runner_outcome']=repr(e);save();raise

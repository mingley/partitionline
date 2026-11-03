#!/usr/bin/env python3
"""Run the unchanged paired queue comparison after the full v17 CI gate."""
import argparse
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import random
import shutil
import subprocess
import sys
import time

import bind_qualification as binding

HERE=Path(__file__).resolve().parent
FLOOR=3*1024**3//2
SPEC_SHA='__V17_SPEC_SHA__'
IMPORT_SHA='560658682de8033e09ba973f2d4be5e493c140665f57c491cae0a1fa5f3e0d72'
QUEUE_GZ_SHA='ead40569dd695873dca6fc8cdf00ca14fd0827f6582c1f55a3a7e0d3e3689086'
V16_PRODUCER='38add9415de68efd3ede5a27ce3c182adea92cbd94cd8c77e07b74774d30f6d1'
V14_PRODUCER='ecf14e7e89fff1d1c29faa6778061d58545f9df565eb7858561d4de58920b279'
V13_PRODUCER='7ce5770e90e850148845eec2958d83705875934a23e889fb492d642219a57a6b'
DEADLINE_SHA='bff6fc134996b97cfc493fd4657f360c911127213ec88f0ca728548c7d9dfeeb'
V16_TESTS='c6696efaa6edbee311d508a391603d5332d52ed58c7ac6a82d0385add1f35712'
V10_PRODUCER='bf165ca682b17f3812d3a3895854cdbf9f73ce862a9330bbc8a6dd2f4675be34'
V10_TESTS='4cd33e8febe735177c7ab5c1124a4a42a9f05af1eb7c6275e614fdeec673c0d5'
SEED=20261103

def sha(data):return hashlib.sha256(data).hexdigest()
def digest(p):
    with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def write_json(path,obj):
    tmp=path.with_suffix(path.suffix+'.tmp');tmp.write_text(json.dumps(obj,indent=2)+'\n');os.replace(tmp,path)
def safe_copy(source,destination,catalog):
    destination.mkdir(parents=True,exist_ok=False)
    for name,expected in sorted(catalog.items()):
        p=Path(name)
        if p.is_absolute() or '..' in p.parts:raise RuntimeError('unsafe catalog path: '+name)
        src=source/p
        if src.is_symlink() or not src.is_file() or not src.resolve().is_relative_to(source) or digest(src)!=expected:
            raise RuntimeError('qualified source drift: '+name)
        dst=destination/p;dst.parent.mkdir(parents=True,exist_ok=True)
        if shutil.disk_usage(dst.parent).free < FLOOR + src.stat().st_size:
            raise RuntimeError('disk reserve unavailable during source copy')
        shutil.copy2(src,dst)

def inventory(root):
    result=subprocess.run(['rg','--files','--hidden','-0','-g','!/target/**','-g','!/docs/**',
        '-g','!**/__pycache__/*.pyc','-g','!**/__pycache__/*.pyo'],cwd=root,capture_output=True,timeout=120)
    if result.returncode:raise RuntimeError('source inventory failed: '+result.stderr.decode(errors='replace')[:8192])
    return {name.decode():digest(root/name.decode()) for name in sorted(result.stdout.split(b'\0')) if name}

def short_command(args,cwd=None):
    result=subprocess.run(args,cwd=cwd,capture_output=True,text=True,timeout=60)
    if result.returncode:raise RuntimeError('setup command failed '+repr(args)+': '+result.stderr[:8192])
    return result.stdout

def checked_patch(root,patch,reverse):
    flags=['--reverse'] if reverse else []
    for check in (True,False):
        result=subprocess.run(['git','apply',*flags,*(['--check'] if check else []),str(patch)],cwd=root,capture_output=True,text=True,timeout=120)
        if result.returncode:raise RuntimeError('git apply failed: '+result.stderr[:8192])

def inject(root):
    producer=root/'src/producer.rs';s=producer.read_text()
    anchor=s.index('async fn spawn_slot_worker(')
    start=s.index('    let worker = Worker {',anchor)
    end=s.index('\n    let handle = tokio::spawn(worker.run());',start)
    constructor=s[start:end]
    if not constructor.endswith('    };'):raise RuntimeError('unrecognized Worker initializer')
    constructor=constructor.replace('    let worker = Worker {','    let mut worker = Worker {',1)
    # The original spawn function receives &Arc<Shared>; this fixture owns an
    # Arc<Shared>, so both arms must borrow it for the same lease constructor.
    lease='WorkerLease::register(shared)'
    if constructor.count(lease)!=1:raise RuntimeError('unrecognized WorkerLease registration')
    constructor=constructor.replace(lease,'WorkerLease::register(&shared)',1)
    template=(HERE/'pending_queue_bench.rs.in').read_text()
    if template.count('__WORKER_INITIALIZER__')!=1:raise RuntimeError('bad fixture template')
    harness=template.replace('__WORKER_INITIALIZER__',constructor)
    if harness.count(constructor)!=1 or harness.replace(constructor,'__WORKER_INITIALIZER__',1)!=template:
        raise RuntimeError('benchmark templates differ beyond the Worker initializer')
    (root/'src/producer/pending_queue_bench.rs').write_text(harness)
    old='#[cfg(test)]\nfn test_shared('
    new='#[cfg(any(test, feature = "pending-queue-bench"))]\nfn test_shared('
    if s.count(old)!=1:raise RuntimeError('unrecognized test_shared cfg')
    producer.write_text(s.replace(old,new,1)+'\n/// Private, isolated queue-benchmark hook.\n#[doc(hidden)]\n#[cfg(feature = "pending-queue-bench")]\npub mod pending_queue_bench;\n')
    cargo=root/'Cargo.toml';toml=cargo.read_text()
    if toml.count('[features]\n')!=1 or 'pending-queue-bench' in toml:raise RuntimeError('unexpected Cargo features')
    cargo.write_text(toml.replace('[features]\n','[features]\npending-queue-bench = []\n',1))
    (root/'examples/pending_queue_bench.rs').write_bytes((HERE/'example.rs').read_bytes())
    return {'constructor':constructor,'constructor_sha256':sha(constructor.encode()),'harness_sha256':sha(harness.encode()),
            'modified_producer_sha256':digest(producer),'modified_cargo_sha256':digest(cargo)}

def case_plan():
    return ([{'shape':'not_expired','count':n,'chunk':n,'probes':128} for n in (512,4096,8192)] +
        [{'shape':shape,'count':n,'chunk':n,'probes':1} for shape in ('admit_ordered','admit_zigzag') for n in (512,4096)] +
        [{'shape':shape,'count':n,'chunk':chunk,'probes':1}
         for shape in ('ordered','descending','zigzag') for n in (512,4096) for chunk in (1,64)])

def pair_orders(seed=SEED):
    r=random.Random(seed);orders=[('v10-queue','v16')]*5+[('v16','v10-queue')]*5;r.shuffle(orders);return orders

def validate_sample(obj,case):
    zero=('missing','duplicate','invalid','out_of_order','ack_wrong','ack_error','ack_closed','ack_pending','remaining','buffer_bytes')
    details=('missing_ids','duplicate_ids','invalid_positions','out_of_order_ids','ack_wrong_ids','ack_error_ids','ack_closed_ids','ack_pending_ids')
    return isinstance(obj,dict) and all(obj.get(k)==v for k,v in case.items()) and obj.get('valid') is True and \
        all(obj.get(k)==[] for k in details) and \
        obj.get('accepted')==case['count'] and obj.get('returned')==case['count'] and obj.get('ack_ok')==case['count'] and \
        all(type(obj.get(k)) is int and obj[k]==0 for k in zero) and type(obj.get('elapsed_ns')) is int and obj['elapsed_ns']>0

def owned_helper(path):
    spec=importlib.util.spec_from_file_location('candidate_ownership',path);mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod);return mod

def execute_owned(helper,command,cwd,env,out,err,timeout):
    started=time.monotonic();minimum=shutil.disk_usage(cwd).free;reason=None;receipt=None;code=None;owner=None;interrupted=False
    if minimum<FLOOR:return {'exit':None,'stop_reason':'disk_floor_before_start','minimum_disk_free_bytes':minimum,'elapsed_seconds':0,'cleanup':None,'interrupted':False}
    try:
        with out.open('xb') as stdout,err.open('xb') as stderr:
            with helper.owned_command(command,cwd=cwd,env=env,stdout=stdout,stderr=stderr) as owner:
                while not owner.finished():
                    minimum=min(minimum,shutil.disk_usage(cwd).free)
                    if minimum<FLOOR:reason='disk_floor';break
                    if time.monotonic()-started>timeout:reason='timeout';break
                    time.sleep(.02)
    except Exception as error:
        interrupted=isinstance(error,InterruptedError)
        reason=reason or ('observer_error:'+type(error).__name__+': '+str(error))
    finally:
        if owner is not None:
            receipt=owner.cleanup_receipt;code=owner.process.returncode
    return {'exit':code,'stop_reason':reason,'minimum_disk_free_bytes':minimum,'elapsed_seconds':round(time.monotonic()-started,6),'cleanup':receipt,'interrupted':interrupted}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--qualification',type=Path,required=True)
    parser.add_argument('--checkout',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--witness',type=Path,required=True)
    args=parser.parse_args()
    if sys.platform!='linux' or platform.machine()!='x86_64':raise RuntimeError('Linux x86_64 only')
    qual=args.qualification.resolve();checkout=args.checkout.resolve();out=args.output.resolve()
    if out.is_relative_to(checkout) or out.is_relative_to(qual):raise RuntimeError('output must be outside checkout and qualification')
    if shutil.disk_usage(out.parent).free<FLOOR:raise RuntimeError('disk reserve unavailable before preparation')
    out.mkdir(parents=True,exist_ok=False)
    state={'phase':'validating','production_ready':False,'fastest_claim_qualified':False};write_json(out/'state.json',state)
    try:
        context=binding.same_job_context(os.environ,qual,checkout)
        ci_run=context['github_run_id'];ci_attempt=context['github_run_attempt'];ci_sha=context['github_sha']
        witness_path=args.witness.resolve()
        if witness_path!=Path(context['runner_temp'])/'pending-queue-qualification.json':
            raise RuntimeError('witness must come from the fixed same-job path')
        actual=short_command(['git','rev-parse','HEAD'],cwd=checkout).strip()
        if actual!=ci_sha:raise RuntimeError('checkout and triggering SHA differ')
        spec_path=checkout/'docs/evidence/client/candidate-20261030/v17/spec.json'
        if digest(spec_path)!=SPEC_SHA:raise RuntimeError('v17 spec hash differs')
        spec=json.loads(spec_path.read_text());helper_pins=binding.verify_v17_helpers(checkout,spec)
        contract=json.loads((qual/'contract.json').read_text());summary=json.loads((qual/'summary.json').read_text())
        if contract.get('checkout_commit')!=actual or contract.get('spec_sha256')!=SPEC_SHA:raise RuntimeError('qualification contract differs')
        if digest(checkout/'scripts/ci-qualify-client-candidate.py')!=contract.get('driver_sha256'):
            raise RuntimeError('qualification driver differs')
        expected=binding.expected_witness(context,digest(qual/'contract.json'),digest(qual/'summary.json'),
            digest(checkout/'scripts/ci-qualify-client-candidate.py'),digest(HERE/'bind_qualification.py'),
            helper_pins['helper_manifest_sha256'],helper_pins['helper_guard_sha256'])
        if not binding.validate_witness(json.loads(witness_path.read_text()),expected):
            raise RuntimeError('same-job qualification witness is stale or does not match')
        shutil.copyfile(witness_path,out/'qualification-witness.json')
        if summary.get('passed') is not True or summary.get('baseline_expectations_matched') is not True:
            raise RuntimeError('v17 qualification did not pass')
        if summary.get('required_library_controls')!=166 or summary.get('required_integration_controls')!=17:
            raise RuntimeError('unexpected required control counts')
        if not summary.get('gates') or not all(g.get('passed') is True for g in summary['gates']):raise RuntimeError('qualification gate is incomplete')
        binding.require_complete_gates(summary,spec,binding.load_pinned_controls(spec_path,spec))
        fixed_path=spec_path.parent/spec['fixed_inputs']['path']
        if digest(fixed_path)!=spec['fixed_inputs']['sha256']:raise RuntimeError('fixed catalog drift')
        fixed=json.loads(fixed_path.read_text());source=qual/'source'
        if fixed!=json.loads((HERE/'v16-fixed-inputs.json').read_text()):raise RuntimeError('frozen v16 source inventory differs')
        if inventory(source)!=fixed:raise RuntimeError('qualification source is not the exact fixed catalog')
        if fixed['src/producer.rs']!=V16_PRODUCER or fixed['src/producer/prepare_tests.rs']!=V16_TESTS:
            raise RuntimeError('unexpected v16 producer preimages')
        # Fail if the target or patch is not the reviewed exact input.
        fix=checkout/'docs/evidence/client/candidate-20261030/v14/pending-work-import.patch'
        deadline=spec_path.parent/'deadline-fixture.patch';patch_gz=HERE/'queue-v2.patch.gz'
        if digest(fix)!=IMPORT_SHA or digest(deadline)!=DEADLINE_SHA or digest(patch_gz)!=QUEUE_GZ_SHA:raise RuntimeError('patch hash differs')
        raw=gzip.decompress(patch_gz.read_bytes())
        if sha(raw)!='67a4ead6f405c4822073dccb57f601721d67b3f7265d5f133400df8a8e4f5402':raise RuntimeError('queue patch contents differ')
        raw_patch=out/'queue-v2.patch';raw_patch.write_bytes(raw)
        roots={name:out/name/'source' for name in ('v10-queue','v16')}
        for root in roots.values():safe_copy(source,root,fixed)
        checked_patch(roots['v10-queue'],deadline,True)
        if digest(roots['v10-queue']/'src/producer.rs')!=V14_PRODUCER:raise RuntimeError('deadline fixture reversal differs')
        checked_patch(roots['v10-queue'],fix,True)
        if digest(roots['v10-queue']/'src/producer.rs')!=V13_PRODUCER:raise RuntimeError('test import reversal differs')
        checked_patch(roots['v10-queue'],raw_patch,True)
        before=inventory(roots['v10-queue']);changed={n for n in set(fixed)|set(before) if fixed.get(n)!=before.get(n)}
        if changed!={'src/producer.rs','src/producer/prepare_tests.rs'} or before['src/producer.rs']!=V10_PRODUCER or before['src/producer/prepare_tests.rs']!=V10_TESTS:
            raise RuntimeError('v10 queue control differs beyond the two reviewed source files')
        if before!=json.loads((HERE/'v10-queue-inputs.json').read_text()):raise RuntimeError('frozen v10 queue inventory differs')
        write_json(out/'v16-fixed-inputs.json',fixed);write_json(out/'v10-queue-inputs.json',before)
        checked_patch(roots['v10-queue'],raw_patch,False);checked_patch(roots['v10-queue'],fix,False);checked_patch(roots['v10-queue'],deadline,False)
        if inventory(roots['v10-queue'])!=fixed:raise RuntimeError('queue patch forward roundtrip differs')
        checked_patch(roots['v10-queue'],deadline,True)
        if digest(roots['v10-queue']/'src/producer.rs')!=V14_PRODUCER:raise RuntimeError('deadline fixture reversal differs')
        checked_patch(roots['v10-queue'],fix,True)
        if digest(roots['v10-queue']/'src/producer.rs')!=V13_PRODUCER:raise RuntimeError('test import reversal differs')
        checked_patch(roots['v10-queue'],raw_patch,True)
        if inventory(roots['v10-queue'])!=before:raise RuntimeError('queue patch reverse roundtrip differs')
        adapters={name:inject(root) for name,root in roots.items()}
        extra=('        pending_deadlines_ordered: true,\n'
               '        pending_stats_valid: true,\n'
               '        #[cfg(test)]\n'
               '        pending_work: Arc::new(PendingWork::default()),\n')
        if adapters['v16']['constructor'].count(extra)!=1 or adapters['v16']['constructor'].replace(extra,'',1)!=adapters['v10-queue']['constructor']:
            raise RuntimeError('Worker fixture constructors differ beyond the reviewed queue fields')
        for name,adapter in adapters.items():
            constructor=adapter.pop('constructor')
            if constructor!=(HERE/(name+'-worker-initializer.rs.txt')).read_text():raise RuntimeError('frozen Worker initializer differs: '+name)
            (out/(name+'-worker-initializer.rs.txt')).write_text(constructor)
        injected={name:inventory(root) for name,root in roots.items()}
        hook_changed={n for n in set(injected['v16'])|set(injected['v10-queue']) if injected['v16'].get(n)!=injected['v10-queue'].get(n)}
        if hook_changed!={'src/producer.rs','src/producer/prepare_tests.rs','src/producer/pending_queue_bench.rs'}:
            raise RuntimeError('unexpected post-hook source differences')
        for name,items in injected.items():
            if items!=json.loads((HERE/(name+'-hook-inputs.json')).read_text()):raise RuntimeError('frozen hook inventory differs: '+name)
            write_json(out/(name+'-hook-inputs.json'),items)
        if adapters['v10-queue']['modified_cargo_sha256']!=adapters['v16']['modified_cargo_sha256']:
            raise RuntimeError('benchmark Cargo settings differ')
        if digest(roots['v10-queue']/'Cargo.lock')!=digest(roots['v16']/'Cargo.lock'):raise RuntimeError('lockfiles differ')
        helper_path=checkout/'scripts/candidate-command-ownership.py'
        if digest(helper_path)!=contract['ownership_helper_sha256']:raise RuntimeError('ownership helper drift')
        helper=owned_helper(helper_path)
        eligible=sorted(os.sched_getaffinity(0))
        if not eligible:raise RuntimeError('empty allowed CPU set')
        chosen_cpu=eligible[0];os.sched_setaffinity(0,{chosen_cpu})
        if os.sched_getaffinity(0)!={chosen_cpu}:raise RuntimeError('CPU affinity was not applied')
        governor_file=Path(f'/sys/devices/system/cpu/cpu{chosen_cpu}/cpufreq/scaling_governor')
        governor=governor_file.read_text().strip() if governor_file.exists() else 'unavailable'
        env=os.environ.copy()
        for key in list(env):
            if key in ('RUSTFLAGS','RUSTDOCFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','RUSTUP_TOOLCHAIN','RUSTC','RUSTDOC','CARGO_BUILD_TARGET','RUST_MIN_STACK','RUST_TEST_THREADS') or key.startswith(('CARGO_PROFILE_RELEASE_','CARGO_TARGET_')):
                env.pop(key,None)
        env.update(CARGO_BUILD_JOBS='1',CARGO_INCREMENTAL='0',CARGO_TERM_COLOR='never',PYTHONDONTWRITEBYTECODE='1')
        info={'github_run':ci_run,'github_attempt':ci_attempt,'checkout_sha':actual,'spec_sha256':SPEC_SHA,
              'qualification_contract_sha256':digest(qual/'contract.json'),'qualification_summary_sha256':digest(qual/'summary.json'),
              'same_job_witness_sha256':digest(witness_path),'same_job_witness_scope':'workflow ordering; not an independent signature',
              'fixed_catalog_sha256':digest(fixed_path),'control_changed_paths':sorted(changed),'hook_changed_paths':sorted(hook_changed),'adapters':adapters,
              'harness_template_sha256':digest(HERE/'pending_queue_bench.rs.in'),'example_sha256':digest(HERE/'example.rs'),
              'queue_patch_sha256':sha(raw),'deadline_fixture_patch_sha256':digest(deadline),'import_patch_sha256':digest(fix),
              **helper_pins,'helper_sha256':digest(helper_path),'runner_sha256':digest(Path(__file__)),
              'platform':platform.platform(),'architecture':platform.machine(),
              'allowed_cpus_before_pin':eligible,'pinned_cpu':chosen_cpu,'cpu_governor':governor,
              'cpuinfo':Path('/proc/cpuinfo').read_text(errors='replace'),
              'rustc':short_command(['rustc','+stable','-Vv']),
              'cargo':short_command(['cargo','+stable','-V']),
              'cases':case_plan(),'pair_orders':pair_orders(),'seed':SEED,'warmup_pairs':1,'measurement_pairs':10,
              'build_command':['cargo','+stable','build','--release','--locked','--features','pending-queue-bench','--example','pending_queue_bench'],
              'limits':['Queue microbenchmark with synthetic acknowledgements; no Kafka transport or durability.','Admission samples call push_pending one record at a time; recv_many/account_pending_from and sticky policy are not measured.','Hosted x86 runner measurements are noisy and do not establish fastest ranking.','The harness uses release cfg, with test-only atomic witnesses excluded.','Same-job qualification witness depends on trusted workflow order and is not independently signed.','Helper verification checks pinned bytes, not dependencies or live execution.']}
        write_json(out/'contract.json',info)
        state['phase']='building';write_json(out/'state.json',state)
        binaries={}
        for name in ('v10-queue','v16'):
            e=env.copy();e['CARGO_TARGET_DIR']=str(out/name/'target')
            result=execute_owned(helper,info['build_command'],roots[name],e,out/(name+'-build.stdout.log'),out/(name+'-build.stderr.log'),1200)
            write_json(out/(name+'-build.json'),result)
            if result['exit']!=0 or result['stop_reason'] or result['cleanup'] is None or result['cleanup']['remaining_descendants']:
                raise RuntimeError('release build failed for '+name)
            binaries[name]=out/name/'target/release/examples/pending_queue_bench'
            if not binaries[name].is_file():raise RuntimeError('missing compiled example')
        binary_info={name:{'sha256':digest(path),'bytes':path.stat().st_size} for name,path in binaries.items()}
        for name,root in roots.items():
            if inventory(root)!=injected[name]:raise RuntimeError('source changed during build: '+name)
        if short_command(['rustc','+stable','-Vv'])!=info['rustc'] or short_command(['cargo','+stable','-V'])!=info['cargo']:
            raise RuntimeError('stable compiler or Cargo changed during builds')
        write_json(out/'binaries.json',binary_info)
        state['phase']='sampling';write_json(out/'state.json',state)
        (out/'samples').mkdir();sequence=0
        with (out/'samples.jsonl').open('x',buffering=1) as samples:
            for case_index,case in enumerate(case_plan()):
                orders=[('v10-queue','v16')]+pair_orders(SEED+case_index)
                for index,order in enumerate(orders):
                    pair_valid=True
                    for position,name in enumerate(order):
                        sequence+=1;tag=f'{sequence:04d}-{name}';stdout=out/'samples'/f'{tag}.stdout';stderr=out/'samples'/f'{tag}.stderr'
                        command=[str(binaries[name]),case['shape'],str(case['count']),str(case['chunk']),str(case['probes'])]
                        result=execute_owned(helper,command,roots[name],env,stdout,stderr,60)
                        stdout_data=stdout.read_bytes() if stdout.exists() else b''
                        stderr_data=stderr.read_bytes() if stderr.exists() else b''
                        stdout_text=stdout_data.decode('utf-8',errors='replace')
                        try:parsed=json.loads(stdout_text)
                        except (ValueError,TypeError):parsed=None
                        valid=result['exit']==0 and result['stop_reason'] is None and result['cleanup'] is not None and not result['cleanup']['remaining_descendants'] and digest(binaries[name])==binary_info[name]['sha256'] and validate_sample(parsed,case)
                        row={'case_index':case_index,'case':case,'pair_index':index-1,'warmup':index==0,'pair_order':list(order),
                             'position':position,'arm':name,'binary_sha256':digest(binaries[name]),'command':command,
                             'stdout_path':str(stdout.relative_to(out)),'stderr_path':str(stderr.relative_to(out)),
                             'stdout_sha256':sha(stdout_data),'stderr_sha256':sha(stderr_data),
                             'process':result,'sample':parsed,'valid':valid,'loadavg':Path('/proc/loadavg').read_text().strip()}
                        samples.write(json.dumps(row,sort_keys=True)+'\n');samples.flush()
                        if result['interrupted']:raise InterruptedError('sample interrupted; partial pair retained')
                        pair_valid=pair_valid and valid
                    if not pair_valid:raise RuntimeError(f'invalid or failed sample pair case={case_index} pair={index-1}; raw data retained')
        for name,root in roots.items():
            if inventory(root)!=injected[name]:raise RuntimeError('source changed during samples: '+name)
        state.update(phase='complete',samples_sha256=digest(out/'samples.jsonl'),scope='queue microbenchmark only; no performance ranking')
        write_json(out/'state.json',state)
        return 0
    except BaseException as error:
        state.update(phase='failed',error_type=type(error).__name__,error=str(error));write_json(out/'state.json',state)
        print(str(error),file=sys.stderr);return 1

if __name__=='__main__':raise SystemExit(main())

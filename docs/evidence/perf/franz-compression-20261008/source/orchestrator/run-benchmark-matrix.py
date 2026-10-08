#!/usr/bin/env python3
"""Plan and run paired benchmark diagnostics with retained attempts and owned topics."""
from __future__ import annotations

import argparse
import ctypes
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import re
import signal
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
MAX_JSON = 4 * 1024 * 1024
MAX_LOG = 16 * 1024 * 1024
MAX_HISTORY = 64 * 1024 * 1024
EVENT_HEAD = {}
SAFE_ENV = set('COUNT WARMUP PAYLOAD_BYTES PARTITIONS ACKS IDEMPOTENT LINGER_MS BATCH_BYTES BATCH_RECORDS MAX_IN_FLIGHT QUEUE_MESSAGES QUEUE_KBYTES DELIVERY_TIMEOUT_MS FLUSH_TIMEOUT_MS RUN_TIMEOUT_MS CONSUME_TIMEOUT_MS RECORD_SEED LATENCY_SAMPLES COMPRESSION ISOLATION PAYLOAD_MODE KEY_MODE ZSTD_LEVEL SECURITY_PROTOCOL SASL_MECHANISM RTT_MS NETWORK_INTERFACE BENCH_ENVIRONMENT'.split())
MATCH = 'acks idempotence isolation_level security_protocol compression linger_ms batch_size_bytes batch_num_messages max_in_flight queue_max_messages queue_max_kbytes delivery_timeout_ms flush_timeout_ms run_timeout_ms consume_timeout_ms count warmup payload_bytes partitions record_seed payload_mode key_mode partitioner connections_per_broker socket_nagle_disable'.split()


class Interrupted(Exception):
    pass


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as file:
        for block in iter(lambda: file.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def duplicate_guard(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError('duplicate JSON key: ' + key)
        result[key] = value
    return result


def read(path):
    if Path(path).stat().st_size > MAX_JSON:
        raise ValueError('JSON size limit')
    return json.loads(Path(path).read_text(), object_pairs_hook=duplicate_guard)


def atomic(path, data):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('x') as file:
        json.dump(data, file, indent=2); file.write('\n'); file.flush(); os.fsync(file.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def name(value):
    return isinstance(value, str) and re.fullmatch(r'[a-z0-9][a-z0-9-]{0,47}', value)


def argv(value):
    if not isinstance(value, list) or not 1 <= len(value) <= 64 or any(not isinstance(s, str) or not s or len(s) > 4096 or '\x00' in s for s in value):
        raise ValueError('bounded argument vector required')
    return value


def validate(manifest):
    if manifest.get('schema_version') != 1 or type(manifest.get('seed')) is not int:
        raise ValueError('schema_version1 and integer seed required')
    if type(manifest.get('repetitions')) is not int or not 1 <= manifest['repetitions'] <= 25:
        raise ValueError('repetitions outside1..25')
    if not 0.1 <= manifest.get('timeout_seconds', 0) <= 600:
        raise ValueError('process timeout outside0.1..600 seconds')
    peers = manifest.get('peers', [])
    if len(peers) != 2 or len({p.get('id') for p in peers}) != 2:
        raise ValueError('exactly two distinct named A/B arms required')
    for peer in peers:
        if not name(peer['id']): raise ValueError('invalid peer ID')
        argv(peer['command']); argv(peer['emit_config'])
        if not peer.get('inputs'): raise ValueError('hashed peer inputs required')
        for pin in peer['inputs']:
            if not Path(pin['path']).is_absolute() or not re.fullmatch('[0-9a-f]{64}', pin['sha256']):
                raise ValueError('input needs absolute path and SHA256')
    broker_kind = manifest['broker'].get('kind', 'kafka')
    if broker_kind not in ('kafka', 'null-broker'):
        raise ValueError('broker kind must be kafka or null-broker')
    cells = manifest.get('cells', [])
    if not 1 <= len(cells) <= 32 or len({c.get('id') for c in cells}) != len(cells):
        raise ValueError('unique cells required, maximum32')
    for cell in cells:
        if not name(cell['id']) or not isinstance(cell.get('env'), dict): raise ValueError('invalid cell')
        target_kind = cell.get('target_kind', broker_kind)
        result_kind = cell.get('result_kind', 'client-ceiling' if target_kind == 'null-broker' else 'kafka-throughput')
        if target_kind != broker_kind:
            raise ValueError('cell target kind differs from the inspected broker kind')
        if result_kind != ('client-ceiling' if broker_kind == 'null-broker' else 'kafka-throughput'):
            raise ValueError('client-ceiling and Kafka-throughput results need separate manifests')
        if broker_kind == 'kafka' and cell['id'].startswith(('nb-', 'ceiling-exp-null-')):
            raise ValueError('null-broker cells cannot be registered as Kafka-throughput cells')
        if broker_kind == 'null-broker':
            raise ValueError('client-ceiling adapters are not qualified for this orchestrator yet')
        if set(cell['env']) - SAFE_ENV or any(not isinstance(v, str) or len(v) > 1024 for v in cell['env'].values()):
            raise ValueError('cell environment must use declared nonsecret settings')
        if int(cell['env'].get('WARMUP', '0')) < 1: raise ValueError('explicit positive warmup required')
        if not isinstance(cell.get('equal_semantics'), dict): raise ValueError('declared durability/security required')
        for peer, reason in cell.get('unsupported', {}).items():
            if peer not in {p['id'] for p in peers} or not isinstance(reason, str) or not reason:
                raise ValueError('unsupported arms need an explicit reason')
    broker = manifest['broker']
    if any(not isinstance(broker.get(k), str) or not broker[k] for k in ['bootstrap', 'image', 'version', 'cluster_id']):
        raise ValueError('inspected broker identity required')
    for key in ['identity', 'create', 'delete']:
        argv(manifest['provision'][key])
    if '{topic}' not in manifest['provision']['create'] or '{topic}' not in manifest['provision']['delete']:
        raise ValueError('provisioning must name the exact owned topic')
    if not isinstance(manifest['provision'].get('identity_stdout'), str):
        raise ValueError('exact inspected identity output required')
    check_pins(manifest)


def check_pins(manifest):
    for peer in manifest['peers']:
        for pin in peer['inputs']:
            if sha(pin['path']) != pin['sha256']: raise ValueError('peer input pin differs: ' + pin['path'])


def expand(command, values):
    result = []
    for arg in argv(command):
        for key, value in values.items(): arg = arg.replace('{' + key + '}', str(value))
        if re.search(r'\{[a-z_]+\}', arg): raise ValueError('unknown command placeholder')
        result.append(arg)
    return result


def base_env():
    env = os.environ.copy()
    for key in list(env):
        if key in SAFE_ENV or key.startswith(('KAFKA_', 'SASL_', 'SSL_', 'TLS_', 'C_PEER_', 'JAVA_PEER_')) or key in ['LD_PRELOAD', 'LD_LIBRARY_PATH', 'JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS', 'CLASSPATH', 'PYTHONPATH', 'PYTHONHOME']:
            env.pop(key, None)
    return env


def group_members(group):
    result = []
    for path in Path('/proc').glob('[0-9]*/stat'):
        try:
            fields = path.read_text().rsplit(')', 1)[1].split()
            if int(fields[2]) == group: result.append(int(path.parent.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
    return result


def stop_group(child):
    for sig in [signal.SIGTERM, signal.SIGKILL]:
        try: os.killpg(child.pid, sig)
        except ProcessLookupError: pass
        try: child.wait(timeout=.3)
        except subprocess.TimeoutExpired: pass
        if sig == signal.SIGTERM and not group_members(child.pid): break
    child.wait(timeout=3)
    reaped = []
    deadline = time.monotonic() + 3
    while True:
        try:
            pid, status = os.waitpid(-child.pid, os.WNOHANG)
            if pid: reaped.append(dict(pid=pid,exit_code=os.waitstatus_to_exitcode(status))); continue
        except ChildProcessError:
            pass
        if not group_members(child.pid): return reaped
        if time.monotonic() > deadline: raise RuntimeError('owned process group did not drain')
        time.sleep(.01)


def execute(command, env, directory, label, timeout):
    output = directory / (label + '.stdout'); errors = directory / (label + '.stderr')
    receipt = dict(command=command, started=time.time(), parent_waited=False)
    child = None; failure = None
    with output.open('xb') as stdout, errors.open('xb') as stderr:
        try:
            child = subprocess.Popen(command,env=env,stdout=stdout,stderr=stderr,start_new_session=True)
            receipt['pid'] = child.pid; deadline = time.monotonic() + timeout
            while child.poll() is None:
                if time.monotonic() > deadline: raise TimeoutError('process deadline')
                if output.stat().st_size > MAX_LOG or errors.stat().st_size > MAX_LOG: raise ValueError('process log limit')
                time.sleep(.02)
            receipt['exit_code'] = child.wait(); receipt['parent_waited'] = True
            if group_members(child.pid): raise RuntimeError('adapter left child processes running')
        except BaseException as error:
            failure = error; receipt['failure'] = type(error).__name__
            if child:
                receipt['adopted_children'] = stop_group(child)
                receipt.update(exit_code=child.returncode,parent_waited=True)
        finally:
            receipt['ended'] = time.time()
            stdout.flush(); stderr.flush(); os.fsync(stdout.fileno()); os.fsync(stderr.fileno())
            receipt['artifacts'] = {p.name:sha(p) for p in [output,errors]}
            atomic(directory / (label + '.process.json'), receipt)
    if failure: raise failure
    if receipt['exit_code']: raise ValueError('command failed; retained ' + label + ' process receipt')
    return output


def event(output, value):
    if output not in EVENT_HEAD: events(output)
    previous=EVENT_HEAD[output]
    encoded=json.dumps(value,sort_keys=True,separators=(',',':'))
    digest=hashlib.sha256((previous+encoded).encode()).hexdigest()
    with (output/'events.jsonl').open('a') as file:
        file.write(json.dumps(dict(previous=previous,event=value,sha256=digest),sort_keys=True)+'\n');file.flush();os.fsync(file.fileno())
    EVENT_HEAD[output]=digest


def events(output):
    path = output/'events.jsonl'
    previous=sha(output/'plan.json');result=[]
    if path.exists():
        if path.stat().st_size > MAX_HISTORY: raise ValueError('event history limit')
        for line in path.read_text().splitlines():
            row=json.loads(line,object_pairs_hook=duplicate_guard)
            encoded=json.dumps(row['event'],sort_keys=True,separators=(',',':'))
            digest=hashlib.sha256((previous+encoded).encode()).hexdigest()
            if row['previous']!=previous or row['sha256']!=digest:raise ValueError('event history changed')
            result.append(row['event']);previous=digest
    EVENT_HEAD[output]=previous
    return result


def prepare(manifest, output, manifest_path):
    validate(manifest); output.mkdir(parents=True,exist_ok=False)
    rng = random.Random(manifest['seed']); run_id = uuid.uuid4().hex; rows = []
    for repetition in range(1,manifest['repetitions']+1):
        cells=list(manifest['cells']);rng.shuffle(cells)
        for cell in cells:
            arms=list(manifest['peers']);rng.shuffle(arms)
            order='then'.join(p['id'] for p in arms)
            for peer in arms: rows.append(dict(index=len(rows),cell=cell['id'],peer=peer['id'],repetition=repetition,order=order))
    pins={str(Path(__file__).resolve()):sha(__file__),str(ROOT/'scripts/benchmark-report.py'):sha(ROOT/'scripts/benchmark-report.py'),str(ROOT/'benchmarks/result-schema.json'):sha(ROOT/'benchmarks/result-schema.json')}
    atomic(output/'manifest.json',manifest)
    plan=dict(schema_version=1,run_id=run_id,topic_prefix='pl-matrix-'+run_id,seed=manifest['seed'],rows=rows,manifest_sha256=sha(manifest_path),source_pins=pins,scope='Paired diagnostics; no campaign or ranking qualification',suite_hold='active')
    atomic(output/'plan.json',plan)
    atomic(output/'frozen.json',dict(plan_sha256=sha(output/'plan.json'),manifest_sha256=sha(output/'manifest.json')))
    return plan


def settings(config):
    missing = [k for k in MATCH if k not in config]
    if missing: raise ValueError('peer lacks required comparable settings: ' + ', '.join(missing))
    return {k:config[k] for k in MATCH}


def run(manifest, plan, output, retry_failed):
    import jsonschema
    spec=importlib.util.spec_from_file_location('matrix_report',ROOT/'scripts/benchmark-report.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    validator=module.BenchmarkValidator();schema=jsonschema.Draft7Validator(read(ROOT/'benchmarks/result-schema.json'))
    peers={p['id']:p for p in manifest['peers']};cells={c['id']:c for c in manifest['cells']};seen=events(output)
    terminal={e['row']:e for e in seen if e['state'] in ['executed','failed','unsupported']}
    comparisons={};failures=0
    env0=base_env();values0={'bootstrap':manifest['broker']['bootstrap']}
    identity=output/('identity-'+uuid.uuid4().hex);identity.mkdir()
    path=execute(expand(manifest['provision']['identity'],values0),env0,identity,'identity',manifest['timeout_seconds'])
    if path.read_text().strip()!=manifest['provision']['identity_stdout']:raise ValueError('broker identity differs; no topic operation permitted')
    for row in plan['rows']:
        index=row['index'];cell=cells[row['cell']];peer=peers[row['peer']]
        previous=terminal.get(index)
        if previous and (previous['state']!='failed' or not retry_failed):
            for artifact,h in previous.get('artifacts',{}).items():
                if sha(output/artifact)!=h:raise ValueError('retained row artifact changed')
            if previous['state']=='failed':failures+=1
            if previous['state']=='executed':comparisons[(row['cell'],row['repetition'],row['peer'])]=previous['settings']
            continue
        reason=cell.get('unsupported',{}).get(row['peer'])
        if reason:event(output,dict(row=index,state='unsupported',reason=reason));continue
        number=1+sum(e['state']=='started' and e['row']==index for e in seen)
        directory=output/'attempts'/f'{index:04d}-{number}';directory.mkdir(parents=True,exist_ok=False)
        topic=plan['topic_prefix']+f'-{index}-{number}'
        result=directory/'result.json';values=dict(values0,topic=topic,result=result)
        env=dict(env0,**cell['env'],KAFKA_BOOTSTRAP=values['bootstrap'],KAFKA_TOPIC=topic,SCENARIO_ID=cell['id'],REPETITION_INDEX=str(row['repetition']),TOTAL_REPETITIONS=str(manifest['repetitions']),PAIRING_ORDER=row['order'],BROKER_IMAGE=manifest['broker']['image'],BROKER_VERSION=manifest['broker']['version'])
        state=dict(row=index,attempt=number,state='started',topic=topic,directory=str(directory.relative_to(output)));event(output,state);seen.append(state)
        created=False;outcome=dict(row=index,attempt=number,state='failed',topic=topic)
        try:
            check_pins(manifest)
            config=read(execute(expand(peer['emit_config'],values),env,directory,'config',manifest['timeout_seconds']))
            effective=settings(config)
            declared=cell['equal_semantics']
            if (effective['acks']!=module.normalize_acks(declared['acks']) or
                effective['idempotence']!=declared['idempotence'] or
                effective['isolation_level']!=declared['isolation'] or
                effective['security_protocol']!=declared['security']['protocol'] or
                (config.get('sasl_mechanism') or 'NONE')!=declared['security']['mechanism']):
                raise ValueError('actual durability/security settings differ before provisioning')
            numeric_fields={'COUNT':'count','WARMUP':'warmup','PARTITIONS':'partitions','PAYLOAD_BYTES':'payload_bytes','RECORD_SEED':'record_seed','LINGER_MS':'linger_ms','BATCH_BYTES':'batch_size_bytes','BATCH_RECORDS':'batch_num_messages','MAX_IN_FLIGHT':'max_in_flight','QUEUE_MESSAGES':'queue_max_messages','QUEUE_KBYTES':'queue_max_kbytes','DELIVERY_TIMEOUT_MS':'delivery_timeout_ms','FLUSH_TIMEOUT_MS':'flush_timeout_ms','RUN_TIMEOUT_MS':'run_timeout_ms','CONSUME_TIMEOUT_MS':'consume_timeout_ms'}
            for environment,setting in numeric_fields.items():
                if environment in cell['env'] and int(cell['env'][environment],0)!=effective[setting]:
                    raise ValueError('actual workload differs from declared '+environment)
            for environment,setting in [('COMPRESSION','compression'),('PAYLOAD_MODE','payload_mode'),('KEY_MODE','key_mode')]:
                if environment in cell['env'] and cell['env'][environment]!=effective[setting]:
                    raise ValueError('actual workload differs from declared '+environment)
            key=(row['cell'],row['repetition'])
            other=[v for (c,r,p),v in comparisons.items() if (c,r)==key and p!=row['peer']]
            if other and other[0]!=effective:raise ValueError('A/B effective settings differ; refusing this arm before topic creation')
            create=execute(expand(manifest['provision']['create'],values),env,directory,'create',manifest['timeout_seconds']);created=True
            event(output,dict(row=index,attempt=number,state='created',topic=topic))
            execute(expand(peer['command'],values),env,directory,'peer',manifest['timeout_seconds'])
            check_pins(manifest)
            data=read(result)
            if data.get('client_ceiling') is not None or data.get('provenance', {}).get('broker', {}).get('mode') == 'null':
                raise ValueError('client-ceiling result cannot enter a Kafka-throughput cell')
            schema.validate(data);valid,errors,_=validator.validate(data)
            if not valid:raise ValueError('result rejected: '+ '; '.join(errors[:5]))
            if data['scenario']['scenario_id']!=cell['id'] or data['scenario']['equal_semantics']!=cell['equal_semantics']:raise ValueError('durability/security differs from declared cell')
            if data['provenance']['broker']['cluster_id']!=manifest['broker']['cluster_id']:raise ValueError('result broker identity differs')
            if settings(data['provenance']['config']['effective_settings'])!=effective:raise ValueError('executed settings differ from actual preflight')
            if not data['integrity']['verified'] or any(data['outcomes'][k]!=effective['count'] for k in ['offered','accepted','acknowledged','consumed']):raise ValueError('incomplete delivery or receipt audit')
            if not data['execution']['warmup_completed'] or data['execution']['warmup_records']!=effective['warmup']:raise ValueError('warmup not completed as declared')
            if data['execution']['repetition_index']!=row['repetition'] or data['execution']['total_repetitions']!=manifest['repetitions'] or data['execution']['pairing_order']!=row['order']:raise ValueError('result repetition provenance differs')
            for artifact in data['provenance']['artifacts']:
                path=Path(artifact['path']).resolve()
                if not path.is_relative_to(directory.resolve()) or sha(path)!=artifact['sha256']:raise ValueError('missing, external or changed raw artifact')
            outcome.update(state='executed',settings=effective,result=str(result.relative_to(output)))
            comparisons[(row['cell'],row['repetition'],row['peer'])]=effective
        except Interrupted:
            outcome.update(state='interrupted',reason='parent interrupted');raise
        except Exception as error:
            outcome['reason']=str(error);failures+=1
        finally:
            outcome['topic_created']=created
            outcome['artifacts']={str(p.relative_to(output)):sha(p) for p in directory.iterdir() if p.is_file()}
            event(output,outcome)
        print(json.dumps({'row':index,'state':outcome['state']}),flush=True)
    rows=events(output);final={e['row']:e for e in rows if e['state'] in ['executed','failed','unsupported','interrupted']}
    retained_failures=sum(e['state']=='failed' for e in rows)
    atomic(output/'summary.json',dict(status='failed' if failures or retained_failures else 'complete',rows=len(plan['rows']),executed=sum(e['state']=='executed' for e in final.values()),failed=sum(e['state']=='failed' for e in final.values()),unsupported=sum(e['state']=='unsupported' for e in final.values()),retained_failed_attempts=retained_failures,retained_interrupted_attempts=sum(e['state']=='interrupted' for e in rows),suite_hold='active',performance_claims_valid=False))
    return int(bool(failures or retained_failures))


def cleanup(manifest,plan,output):
    events(output)
    env=base_env();values={'bootstrap':manifest['broker']['bootstrap']};directory=output/('cleanup-'+uuid.uuid4().hex);directory.mkdir()
    path=execute(expand(manifest['provision']['identity'],values),env,directory,'identity',manifest['timeout_seconds'])
    if path.read_text().strip()!=manifest['provision']['identity_stdout']:raise ValueError('broker identity differs; cleanup refused')
    history=events(output)
    known={e['topic'] for e in history if e['state']=='created' or e.get('topic_created') is True};done={e['topic'] for e in history if e['state']=='deleted'}
    # A signal can arrive after the successful create was durably recorded by
    # execute(), before the caller records its created event.
    for row in history:
        if row['state']!='started':continue
        receipt=output/row['directory']/'create.process.json'
        if receipt.exists():
            data=read(receipt)
            if data.get('parent_waited') and data.get('exit_code')==0:
                expected=expand(manifest['provision']['create'],dict(values,topic=row['topic'],result=output/row['directory']/'result.json'))
                if data['command']!=expected:raise ValueError('create receipt does not match owned plan')
                known.add(row['topic'])
    for index,topic in enumerate(sorted(known-done)):
        if not re.fullmatch(re.escape(plan['topic_prefix'])+r'-\d+-\d+',topic):raise ValueError('topic outside owned namespace')
        execute(expand(manifest['provision']['delete'],dict(values,topic=topic)),env,directory,'delete-'+str(index),manifest['timeout_seconds'])
        event(output,dict(state='deleted',topic=topic))
    return 0


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['plan','run','resume','cleanup'])
    parser.add_argument('--manifest',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--approve-provision',action='store_true');parser.add_argument('--retry-failed',action='store_true')
    args=parser.parse_args();manifest=read(args.manifest);output=args.output.resolve()
    if args.action!='plan' and not args.approve_provision:raise ValueError('review the concrete plan and supply --approve-provision for owned topic operations')
    if not sys.platform.startswith('linux'):raise ValueError('Linux process ownership required')
    if ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0)!=0:raise OSError(ctypes.get_errno(),'subreaper setup failed')
    for sig in [signal.SIGINT,signal.SIGTERM]:signal.signal(sig,lambda *_:(_ for _ in ()).throw(Interrupted()))
    if args.action in ['plan','run']:plan=prepare(manifest,output,args.manifest)
    else:
        plan=read(output/'plan.json')
        frozen=read(output/'frozen.json')
        if sha(output/'plan.json')!=frozen['plan_sha256'] or sha(output/'manifest.json')!=frozen['manifest_sha256']:raise ValueError('frozen plan or manifest changed')
        if plan['manifest_sha256']!=sha(args.manifest) or read(output/'manifest.json')!=manifest:raise ValueError('manifest changed; create a new run')
        for path,h in plan['source_pins'].items():
            if sha(path)!=h:raise ValueError('orchestrator/validator input changed')
        validate(manifest)
    with (output/'.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if args.action=='plan':print(json.dumps(plan,indent=2));return 0
        if args.action=='cleanup':return cleanup(manifest,plan,output)
        return run(manifest,plan,output,args.retry_failed)


if __name__=='__main__':
    try:sys.exit(main())
    except Interrupted:print('matrix interrupted; attempts retained for resume',file=sys.stderr);sys.exit(130)
    except Exception as error:print('matrix: '+str(error),file=sys.stderr);sys.exit(2)

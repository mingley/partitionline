#!/usr/bin/env python3
"""Synthetic old/corrected guard comparison; no /proc, cache or command execution."""
from pathlib import Path as RealPath
import ast,copy,hashlib,json,os,stat,types
BASE=RealPath('/workspace/work/client-capability-qa-preparation-e90efb49');PREP=BASE/'platform-daemon-exception-preparation-v2'
allowraw=(BASE/'platform-daemon-exception-preparation/exact-platform-daemons.json').read_bytes()
modelraw=(PREP/'check-exact-daemon-exception.py').read_bytes()
assert hashlib.sha256(modelraw).hexdigest()=='19b77f6bf5c7457b2b2f2a57efcb6e01bf0a31f4ae87d2a533f4f5c91b606719'
classes=ast.Module(body=[n for n in ast.parse(modelraw).body if isinstance(n,ast.ClassDef) and n.name in {'Model','Node'}],type_ignores=[])
function_names={'verify_platform_daemons','verify_docker_cli','docker_zero_running_workloads','process_reference_guard','forecast'}
rows=[]
for filename,sha,should_reject in [('run-forced-baseline403-platform-exception.py','e0919e0a61cee3d304bff3d822d1bc54c41e412d92f23f7b288951fa3150ba90',False),('run-forced-baseline403-platform-exception-v2.py','231425e452e5f1045b5eb6a8006a459d6f8ed6f4bd3dca3dc44afc4a6b3ba07c',True)]:
    raw=(BASE/filename).read_bytes();assert hashlib.sha256(raw).hexdigest()==sha
    functions=ast.Module(body=[n for n in ast.parse(raw).body if isinstance(n,ast.FunctionDef) and n.name in function_names],type_ignores=[])
    ns={'copy':copy,'hashlib':hashlib,'stat':stat,'os':os,'types':types,'json':json,'ACTUAL_ALLOW':json.loads(allowraw),'ACTUAL_ALLOW_BYTES':allowraw,'CODE':compile(ast.fix_missing_locations(functions),filename,'exec')}
    exec(compile(ast.fix_missing_locations(classes),'synthetic-model-classes','exec'),ns)
    for field in ['cwd','exe','environ','maps','fd']:
        model=ns['Model']();model.faults['/proc/700/'+field]=FileNotFoundError(2,'still-live synthetic process field missing')
        guard=model.namespace()['process_reference_guard'];rejected=False
        try:guard()
        except AssertionError:rejected=True
        assert rejected==should_reject
        rows.append({'runner_sha256':sha,'scenario':'ENOENT_'+field+'_while_same_PID_starttime_live','rejected':rejected,'old_unexpected_acceptance':not should_reject and not rejected,'new_required_rejection':should_reject and rejected})
    model=ns['Model']();modelns=model.namespace();original=model.read;changed=[False]
    def hook(path):
        if path=='/proc/700/maps':
            model.proc(700,'replacement-owner',1,903);changed[0]=True
        return original(path)
    model.read=hook;rejected=False
    try:modelns['process_reference_guard']()
    except AssertionError:rejected=True
    assert changed[0], 'both models must actually observe mutation during field inspection'
    assert rejected==should_reject
    rows.append({'runner_sha256':sha,'scenario':'nonexempt_PID_starttime_comm_changed_after_inspection','rejected':rejected,'old_unexpected_acceptance':not should_reject and not rejected,'new_required_rejection':should_reject and rejected})
result={'schema_version':1,'passed':True,'scope':'synthetic extracted source functions only; original guard accepted6 unsafe model states and revised guard rejected all6; no actual process/cache/Cargo/API behavior proof','source_sha256':hashlib.sha256(RealPath(__file__).read_bytes()).hexdigest(),'cases':rows,'old_guard_unexpected_acceptances':6,'new_guard_required_rejections':6,'actual_runner_workflow_executions':0,'actual_Cargo_commands':0,'actual_Docker_queries':0,'actual_removals':0}
out=PREP/'fail-first-live-field-controls.json';out.write_text(json.dumps(result,indent=2)+'\n');out.chmod(0o600)
print(json.dumps({'passed':True,'old_synthetic_unexpected_acceptances':6,'new_synthetic_rejections':6,'receipt_sha256':hashlib.sha256(out.read_bytes()).hexdigest()}))

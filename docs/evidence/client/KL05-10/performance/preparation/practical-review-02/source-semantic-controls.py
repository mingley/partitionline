"""Pure extracted-source controls; never connect SQLite, inspect /proc or launch/signal a process."""
from pathlib import Path
import ast,errno,hashlib,json,types
root=Path('/workspace/work/client-sticky-performance-practical-review-02')
bench=root/'benchmarks/sticky-partitioner'
def extract(path,names,globals_):
    tree=ast.parse(path.read_text())
    selected=[node for node in tree.body if isinstance(node,(ast.FunctionDef,ast.ClassDef)) and node.name in names]
    assert {node.name for node in selected}==set(names)
    exec(compile(ast.Module(body=selected,type_ignores=[]),str(path)+'#extracted-definitions-only','exec'),globals_)
    return globals_
controls=[]
def control(name,call,expect_error=False):
    try:
        value=call()
    except (AssertionError,RuntimeError) as error:
        assert expect_error,(name,type(error).__name__,str(error))
        controls.append({'name':name,'expected':'refuse/unknown','observed_exception':type(error).__name__,'pass':True})
    else:
        assert not expect_error,(name,'unexpected success',value)
        controls.append({'name':name,'expected':'accepted exact semantic result','observed':value,'pass':True})
class Cursor:
    def __init__(self,row): self.row=row
    def fetchone(self): return self.row
class FakeConnection:
    def __init__(self,override=None): self.calls=[];self.override=override or {};self.values={}
    def execute(self,sql):
        self.calls.append(sql)
        assert sql.startswith('PRAGMA '),'No SQL audit/table or real SQLite call allowed'
        key,value=(sql[7:].split('=',1)+[None])[:2] if '=' in sql else (sql[7:],None)
        if value is not None:
            mapping={'page_size':int(value) if key=='page_size' else None,
                     'max_page_count':int(value) if key=='max_page_count' else None,
                     'journal_mode':'delete','synchronous':2,'cache_spill':0,'temp_store':2,'cache_size':-16384}
            self.values[key]=mapping[key]
        observed=self.override.get(key,self.values.get(key))
        return Cursor(None if observed=='MISSING_ROW' else (observed,))
sql=extract(bench/'audit.py',{'pragma_scalar','configure_database'}, {})
def valid_sql(qualification):
    conn=FakeConnection();result=sql['configure_database'](conn,qualification)
    expected=16777216 if qualification else 2147483648
    assert result['page_bytes']==4096 and result['database_byte_cap']==expected
    assert result['journal_mode']=='delete' and result['cache_spill']==0 and result['temp_store']==2
    assert conn.calls[:2]==['PRAGMA page_size=4096','PRAGMA page_size']
    assert result['rollback_journal_allocation_reserve_bytes']==result['statement_journal_allocation_reserve_bytes']
    return {'byte_cap':expected,'pragma_calls':conn.calls,'journal_reserve':result['rollback_journal_allocation_reserve_bytes'],'statement_reserve':result['statement_journal_allocation_reserve_bytes']}
control('Qualification explicit4096 geometry before first schema',lambda:valid_sql(True))
control('Ranking explicit4096 geometry before first schema',lambda:valid_sql(False))
for page in [512,1024,8192,65536,True,'4096','MISSING_ROW']:
    control('Reject ignored/wrong page_size '+str(page),lambda page=page:sql['configure_database'](FakeConnection({'page_size':page}),True),True)
for key,value in [('journal_mode','wal'),('journal_mode','off'),('synchronous',0),('cache_spill',1),('temp_store',0),('max_page_count',4097),('max_page_count',False)]:
    control('Reject unforecasted '+key+'='+str(value),lambda key=key,value=value:sql['configure_database'](FakeConnection({key:value}),True),True)
class FakeNode:
    def __init__(self,name,raw=None,error=None):self.name=str(name);self.raw=raw;self.error=error
    def __truediv__(self,suffix): assert suffix=='stat';return self
    def read_text(self):
        if self.error: raise self.error
        return self.raw
class FakePath:
    entries=[];enumeration_error=None
    def __init__(self,value): assert value=='/proc'
    def iterdir(self):
        if self.enumeration_error:raise self.enumeration_error
        return iter(self.entries)
class FakeOS:
    def __init__(self):self.calls=[];self.exists={};self.group_missing=False
    def kill(self,pid,signal_):
        assert signal_==0,'Only a mocked existence probe'
        self.calls.append(('existence',pid,signal_))
        state=self.exists.get(pid,'missing')
        if state=='missing':raise ProcessLookupError(errno.ESRCH,'fixture vanished')
        if state=='denied':raise PermissionError(errno.EPERM,'fixture unknown')
        assert state=='alive'
    def killpg(self,pgid,signal_):
        assert signal_==9;self.calls.append(('mock-group-signal',pgid,signal_))
        if self.group_missing:raise ProcessLookupError(errno.ESRCH,'fixture group vanished')
os=FakeOS()
proc=extract(bench/'run-cell.py',{'MembershipInspectionError','members','stop'},{'Path':FakePath,'os':os,'signal':types.SimpleNamespace(SIGKILL=9)})
def stat_record(pid,group,session,start):
    # Linux stat fields3..22: state,ppid,pgrp,session,...,starttime. No actual proc read.
    fields=['S','1',str(group),str(session),'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0',str(start)]
    return str(pid)+' (fixture) '+' '.join(fields)+' 0 0\n'
def fixture(nodes,exists=None,enum_error=None):
    FakePath.entries=nodes;FakePath.enumeration_error=enum_error;os.calls=[];os.exists=exists or {};os.group_missing=False

def inspect(nodes,owner=None,exists=None,enum_error=None):
    fixture(nodes,exists,enum_error);return proc['members'](123,owner)
control('Readable matching PGID/session/starttime',lambda:inspect([FakeNode(123,stat_record(123,123,123,17))],17))
control('Readable nonmatching group ignored',lambda:inspect([FakeNode(500,stat_record(500,500,400,8))],17))
control('Nonnumeric proc metadata skipped',lambda:inspect([FakeNode('self',error=PermissionError())],17))
control('Confirmed vanished PID ENOENT',lambda:inspect([FakeNode(800,error=FileNotFoundError(errno.ENOENT,'fixture'))],17))
control('Confirmed vanished PID ESRCH',lambda:inspect([FakeNode(800,error=ProcessLookupError(errno.ESRCH,'fixture'))],17))
control('Live unreadable owned PID is unknown',lambda:inspect([FakeNode(123,error=PermissionError(errno.EACCES,'fixture'))],17),True)
control('Unrelated unreadable PID cannot prove empty group',lambda:inspect([FakeNode(555,error=PermissionError(errno.EACCES,'fixture'))],17),True)
control('Stat absent but independently live PID is unknown',lambda:inspect([FakeNode(800,error=FileNotFoundError(errno.ENOENT,'fixture'))],17,{800:'alive'}),True)
control('Stat absent but existence permission unknown',lambda:inspect([FakeNode(800,error=FileNotFoundError(errno.ENOENT,'fixture'))],17,{800:'denied'}),True)
control('Other stat IO error is unknown',lambda:inspect([FakeNode(800,error=OSError(errno.EIO,'fixture'))],17),True)
control('Proc enumeration denied is unknown',lambda:inspect([],17,enum_error=PermissionError(errno.EACCES,'fixture')),True)
control('Malformed numeric PID stat is unknown',lambda:inspect([FakeNode(123,'garbage')],17),True)
control('Stat PID identity differs from path',lambda:inspect([FakeNode(123,stat_record(124,123,123,17))],17),True)
control('Session identity differs from owned PGID',lambda:inspect([FakeNode(123,stat_record(123,123,77,17))],17),True)
control('Leader starttime changed despite same PID/PGID',lambda:inspect([FakeNode(123,stat_record(123,123,123,18))],17),True)
control('Descendant predates captured owner',lambda:inspect([FakeNode(124,stat_record(124,123,123,16))],17),True)
control('Descendant newer than captured owner remains member',lambda:inspect([FakeNode(124,stat_record(124,123,123,19))],17))
def stop_case(nodes,owner=17,unknown=False,missing=False):
    fixture(nodes);os.group_missing=missing
    try:result=proc['stop'](123,owner)
    except proc['MembershipInspectionError']:
        assert unknown and not os.calls,'Unverified membership must refuse group signalling'
        return {'group_signal_calls':os.calls,'refused':True}
    assert not unknown
    expected=[('mock-group-signal',123,9)] if nodes else[]
    assert os.calls==expected
    return {'group_signal_calls':os.calls,'verified_members':result}
control('Verified group mock signal permitted',lambda:stop_case([FakeNode(123,stat_record(123,123,123,17))]))
control('Empty verified group sends no signal',lambda:stop_case([]))
control('Permission uncertainty refuses group signal',lambda:stop_case([FakeNode(123,error=PermissionError(errno.EACCES,'fixture'))],unknown=True))
control('Changed leader identity refuses group signal',lambda:stop_case([FakeNode(123,stat_record(123,123,123,18))],unknown=True))
control('Verified group vanished before mock signal',lambda:stop_case([FakeNode(123,stat_record(123,123,123,17))],missing=True))
# Exercise the actual nested wrapper failure helpers with a mocked known direct child.
class FakeProcess:
    pid=123
    def __init__(self):self.direct_kills=0
    def poll(self):return None
    def kill(self):self.direct_kills+=1
run_tree=ast.parse((bench/'run-cell.py').read_text())
helpers=[node for node in ast.walk(run_tree) if isinstance(node,ast.FunctionDef) and node.name in {'inspect_owned','stop_owned'}]
assert {node.name for node in helpers}=={'inspect_owned','stop_owned'}
wrapper=dict(proc);wrapper.update(history=[],reasons=[],owner_start_time=17,process=FakeProcess())
exec(compile(ast.Module(body=helpers,type_ignores=[]),str(bench/'run-cell.py')+'#nested-failure-helpers-only','exec'),wrapper)
def wrapper_case(nodes,stop_=False,recover=False):
    fixture(nodes);wrapper['history']=[];wrapper['reasons']=[];wrapper['process']=FakeProcess()
    first=wrapper['inspect_owned']('fixture inspection')
    if stop_:wrapper['stop_owned']()
    if recover:
        FakePath.entries=[];last=wrapper['inspect_owned']('fixture recovered inspection')
    else:last=first
    accepted=last['membership_verified'] and not last['members'] and not wrapper['reasons']
    if not first['membership_verified']:
        assert first['members'] is None and not accepted,'Unknown cannot be represented/accepted as empty'
        assert wrapper['reasons']
        if stop_:
            assert not os.calls and wrapper['process'].direct_kills==1,'Only mocked known direct child kill after refused group signal'
    elif first['members']:
        assert not accepted
        if stop_:assert os.calls==[('mock-group-signal',123,9)] and not wrapper['process'].direct_kills
    else:assert accepted
    return {'initial':first,'final':last,'would_accept_closure_and_cell':accepted,
            'direct_child_mock_kills':wrapper['process'].direct_kills,
            'group_mock_calls':os.calls,'failure_reasons':wrapper['reasons']}
control('Wrapper unknown membership is null/unverified',lambda:wrapper_case([FakeNode(123,error=PermissionError(errno.EACCES,'fixture'))]))
control('Wrapper unknown stop kills only known direct child',lambda:wrapper_case([FakeNode(123,error=PermissionError(errno.EACCES,'fixture'))],stop_=True))
control('Wrapper recovered empty membership retains prior failure',lambda:wrapper_case([FakeNode(123,error=PermissionError(errno.EACCES,'fixture'))],stop_=True,recover=True))
control('Wrapper changed owner identity retains failure/no group signal',lambda:wrapper_case([FakeNode(123,stat_record(123,123,123,18))],stop_=True))
control('Wrapper verified owned group only group mock signal',lambda:wrapper_case([FakeNode(123,stat_record(123,123,123,17))],stop_=True))
control('Wrapper verified empty group can close without signal',lambda:wrapper_case([]))

# Independent old counterexample geometry and conservative added allocation sums.
q=json.loads((bench/'qualification-resource-forecast.json').read_text());rank=json.loads((bench/'ranking-resource-forecast.json').read_text())
assert sum(q['components'].values())==q['per_cell_generated_allocation_upper_bound_bytes']
assert q['per_cell_generated_allocation_upper_bound_bytes']+q['physical_floor_bytes']+q['stop_margin_bytes']+q['other_lane_growth_reserve_bytes']==687407120==q['per_cell_conservative_free_requirement_bytes']
assert rank['original_requirement_bytes']+rank['SQLite_DELETE_rollback_journal_header_allocation_reserve_bytes']+rank['SQLite_statement_subjournal_allocation_reserve_bytes']==12725083152==rank['per_cell_conservative_free_requirement_bytes']
assert 4096*65536==268435456 and 4096*4096==16777216
result={'classification':'Executed pure extracted-source semantic/fixture controls only; no SQLite connection, actual proc read, signal, subprocess, audit/main or benchmark execution','source_SHA256':{p:hashlib.sha256((bench/p).read_bytes()).hexdigest() for p in ['audit.py','run-cell.py','qualification-resource-forecast.json','ranking-resource-forecast.json']},'control_count':len(controls),'all_pass':True,'controls':controls,'old_page_size_counterexample':{'pages':4096,'legal_page_bytes':65536,'resulting_DB_bytes':268435456,'claimed_old_DB_bytes':16777216},'corrected_qualification_guard_bytes':687407120,'supplemental_ranking_guard_bytes':12725083152}
(root/'source-semantic-controls.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'pure_control_count':len(controls),'all_pass':True,'SQLite_proc_process_or_main_execution':False},indent=2))

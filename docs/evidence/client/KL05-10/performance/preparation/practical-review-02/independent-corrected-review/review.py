"""Independent packet/source review. No SQLite, real proc or process operations."""
import ast
import errno
import hashlib
import json
import os
import stat
from pathlib import Path

ROOT = Path('/workspace/work/client-sticky-performance-practical-review-02')
OLD = Path('/workspace/work/client-sticky-performance-practical-497f')
OUT = Path(__file__).parent
PIN = '2724dd786ec25e9bb371fceaaac6d0deb5f384969f7fa9ba2826d9afb4820ea9'

def identity(p):
    assert p.is_file() and not p.is_symlink()
    s = p.stat()
    return dict(sha256=hashlib.sha256(p.read_bytes()).hexdigest(),bytes=s.st_size,full_mode=stat.S_IMODE(s.st_mode))

def verify():
    mpath = ROOT/'stage-handoff.json'
    assert identity(mpath)['sha256'] == PIN
    m = json.loads(mpath.read_text())
    rows = {}
    assert len(m['files']) == m['payload_paths'] == 114
    for name, expected in m['files'].items():
        assert not Path(name).is_absolute() and '..' not in Path(name).parts
        actual = identity(ROOT/name)
        assert actual == {k:expected[k] for k in actual},name
        rows[name] = actual
    assert sum(v['bytes'] for v in rows.values()) == m['payload_logical_bytes'] == 911443
    for name, mode in m['directories_full_modes'].items():
        p=ROOT/name
        assert p.is_dir() and not p.is_symlink() and stat.S_IMODE(p.stat().st_mode)==mode
    return rows

def main():
    os.sched_setaffinity(0,{2,4})
    before=verify()
    # Replay only the frozen author's extracted-def fixture code. Exclude its
    # final receipt write/print; all Path fixture proc operations use FakePath.
    source=ROOT/'source-semantic-controls.py'
    tree=ast.parse(source.read_text())
    nodes=[]
    for node in tree.body:
        if isinstance(node,ast.Expr) and isinstance(node.value,ast.Call) and isinstance(node.value.func,ast.Attribute) and node.value.func.attr=='write_text':
            break
        nodes.append(node)
    ns={'__name__':'independent_fixture_replay'}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(source)+'#no-receipt-write','exec'),ns)
    assert len(ns['controls'])==44 and all(row['pass'] for row in ns['controls'])
    independent=[]
    def check(name,condition,detail=None):
        assert condition,name
        independent.append(dict(name=name,pass_=True,detail=detail))
    def refuses(name,call):
        try:call()
        except (AssertionError,RuntimeError) as e:check(name,True,dict(exception=type(e).__name__))
        else:raise AssertionError(name+' unexpectedly accepted')
    bench=ROOT/'benchmarks/sticky-partitioner'
    for name in ['StickyBenchmark.java','src/main.rs','Cargo.toml','Cargo.lock','profiles.json','resource-forecast.json']:
        check('preserved public producer/consumer/graph/ranking '+name,identity(bench/name)==identity(OLD/'benchmarks/sticky-partitioner'/name))
    qs=json.loads((bench/'qualification.json').read_text()); oldqs=json.loads((OLD/'benchmarks/sticky-partitioner/qualification.json').read_text())
    qs.pop('SQLite_geometry_and_sidecars')
    check('qualification only adds explicit SQLite geometry/reserves',qs==oldqs)
    om=json.loads((OLD/'stage-handoff.json').read_text())
    for name in om['files']:
        assert identity(OLD/name)==identity(ROOT/'history/frozen-practical-5c94'/name),name
    check('all original 71 frozen payloads preserved',len(om['files'])==71)
    check('old original stage manifest copied exactly',identity(OLD/'stage-handoff.json')==identity(ROOT/'history/frozen-practical-5c94/stage-handoff.json'))
    for qualification,pages in [(True,4096),(False,524288)]:
        result=ns['sql']['configure_database'](ns['FakeConnection'](),qualification)
        db=pages*4096
        # At most every database page represented once with 8 bytes/page plus
        # a conservative 64KiB DELETE-journal header/alignment allowance.
        bound=db+pages*8+65536
        check('separate journal and statement reserves cover conservative geometry '+str(qualification), result['database_byte_cap']==db and result['rollback_journal_allocation_reserve_bytes']>=bound and result['statement_journal_allocation_reserve_bytes']>=bound,dict(database=db,one_old_image_journal_bound=bound,each_reserve=result['rollback_journal_allocation_reserve_bytes']))
    for key,value in [('page_size',4096.0),('synchronous',True),('cache_spill',False),('temp_store','2'),('max_page_count',4096.0)]:
        refuses('strict pragma scalar refuses '+key+' '+repr(value),lambda key=key,value=value:ns['sql']['configure_database'](ns['FakeConnection']({key:value}),True))
    proc=ns['proc']; FakeNode=ns['FakeNode']; stat_record=ns['stat_record']
    # A known member must not be lost when a later unrelated PID is unreadable.
    refuses('known member plus unreadable later PID is unknown',lambda:ns['inspect']([FakeNode(123,stat_record(123,123,123,17)),FakeNode(501,error=PermissionError(errno.EACCES,'mock'))],17))
    refuses('owned member with truncated stat is unknown',lambda:ns['inspect']([FakeNode(123,'123 (mock) S 1 123 123')],17))
    refuses('unknown matching session cannot be a known member',lambda:ns['inspect']([FakeNode(124,stat_record(124,123,124,19))],17))
    check('space-rich stat comm is parsed from final parenthesis',ns['inspect']([FakeNode(123,stat_record(123,123,123,17).replace('(fixture)','(fixture ) (rich)'))],17)[0]['start_time_ticks']==17)
    for error in [PermissionError(errno.EACCES,'mock'),OSError(errno.EIO,'mock')]:
        result=ns['wrapper_case']([FakeNode(123,error=error)],stop_=True,recover=True)
        check('recovered empty preserves '+type(error).__name__+' failure',not result['would_accept_closure_and_cell'] and result['final']['membership_verified'] and result['final']['members']==[] and result['initial']['members'] is None and result['direct_child_mock_kills']==1 and not result['group_mock_calls'])
    q=json.loads((bench/'qualification-resource-forecast.json').read_text());rank=json.loads((bench/'ranking-resource-forecast.json').read_text())
    check('qualification all additive components and explicit floor',sum(q['components'].values())==q['per_cell_generated_allocation_upper_bound_bytes'] and sum(q[k] for k in ['per_cell_generated_allocation_upper_bound_bytes','physical_floor_bytes','stop_margin_bytes','other_lane_growth_reserve_bytes'])==687407120)
    check('ranking original untouched with separate new reserves',rank['original_requirement_bytes']==8396561424 and rank['per_cell_conservative_free_requirement_bytes']==12725083152)
    after=verify();assert before==after
    control=dict(classification='Pure extracted definitions and memory fixtures only; zero SQLite, proc, signals, subprocess, compiler, SDK or benchmark execution',author_fixture_replay_count=44,independent_control_count=len(independent),author_fixture_replay=ns['controls'],independent_controls=independent)
    cp=OUT/'controls.json';cp.write_text(json.dumps(control,indent=2)+'\n');cp.chmod(0o600)
    review=dict(schema_version=1,classification='Independent source-only review of corrected practical benchmark packet; no actual performance or execution qualification',source_manifest=dict(path=str(ROOT/'stage-handoff.json'),**identity(ROOT/'stage-handoff.json')),packet_before=before,packet_after=after,all_114_bytes_full_modes_unchanged=True,controls=dict(path=str(cp),**identity(cp)),source_findings=[dict(original='SQLite page geometry and DELETE sidecars',status='resolved in source',evidence='configure_database explicitly verifies page4096/count, DELETE/FULL/cache_spillOFF/tempMEMORY before schema; separate rollback and statement allocations reflected in effective forecasts'),dict(original='Unreadable process membership could masquerade as empty',status='resolved in source',evidence='Unknown members use null/unverified and persistent failure reasons; group signals refused for unknown identity; only Popen direct child fallback, no inferred descendant closure')],remaining_gates=['Actual offline Cargo resolution/forced compilation on installed immutable source','Actual genuine Java/Rust six qualification histories and lifecycle/mutation proof','Already-ready broker/image cost is a separate physical precondition; forecasts are unobserved estimates','Ranking still requires >=60s and >=1M independent records with five paired runs; qualification supplies no ranking'],no_additional_concrete_source_blocker=True)
    vp=OUT/'validation.json';vp.write_text(json.dumps(review,indent=2)+'\n');vp.chmod(0o600)
    print(json.dumps(dict(validation=identity(vp),controls=identity(cp),independent_controls=len(independent),author_fixture_replays=44),indent=2))

if __name__=='__main__':main()

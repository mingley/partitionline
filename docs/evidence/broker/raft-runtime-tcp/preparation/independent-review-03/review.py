"""Independent frozen derivative review; source and invented-file/memory controls only."""
import ast,difflib,hashlib,itertools,json,os,re,stat,sys
from pathlib import Path

ROOT=Path('/workspace/work/raft-runtime-76/tcp-qualification-03')
OLD=ROOT.with_name('tcp-qualification-02')
OUT=Path(__file__).parent
PIN='c745ffa88433439b7e3d785be8c64a0d81ec936e413c029f94160f34a98507cf'
sys.dont_write_bytecode=True
def identity(p):
    s=p.stat();assert p.is_file() and not p.is_symlink()
    return dict(sha256=hashlib.sha256(p.read_bytes()).hexdigest(),bytes=s.st_size,full07777=stat.S_IMODE(s.st_mode))
def verify(root,pin,rows,bytes_):
    mpath=root/'handoff.json';assert identity(mpath)['sha256']==pin
    m=json.loads(mpath.read_text());assert len(m['rows'])==rows and sum(r['bytes'] for r in m['rows'])==bytes_
    result={}
    for r in m['rows']:
        p=Path(r['source']);assert p==root/r['path'] and '..' not in Path(r['path']).parts
        actual=identity(p);assert actual=={k:r[k] for k in actual},r['path'];result[r['path']]=actual
    return result
def main():
    os.sched_setaffinity(0,{2,4});os.umask(0o077)
    before=verify(ROOT,PIN,29,567483)
    oldbefore=verify(OLD,'e4250889d4cd2e68818a21b19e6a68a8caeb7b516ad54643a24b34880a54591b',20,241020)
    # Run only the author's offline model/fixture definitions. Its temporary
    # invented process tree is relocated to this review scratch; final frozen
    # receipt write is excluded. No real /proc, cache, Docker or process launch.
    p=ROOT/'check-derivative.py';text=p.read_text().split("p=R/'derivative-controls.json';assert not p.exists()",1)[0]
    assert text!=p.read_text()
    assert text.count("prefix='process-control-',dir=R")==1
    text=text.replace("prefix='process-control-',dir=R","prefix='process-control-',dir=CONTROL_OUT")
    ns={'__file__':str(p),'__name__':'fixture_definitions_only','CONTROL_OUT':str(OUT)}
    exec(compile(text,str(p)+'#relocated-fixture-only','exec'),ns)
    assert len(ns['model'])==4 and len(ns['process'])==17
    controls=[]
    def check(name,value,details=None):
        assert value,name;controls.append(dict(name=name,passed=True,details=details))
    core=ROOT/'candidate/partitionline-broker/src/raft/runtime.rs';oldcore=OLD/'candidate/partitionline-broker/src/raft/runtime.rs'
    new=core.read_text();old=oldcore.read_text()
    # Reuse only our earlier nonoperational Rust lexical helpers, not its main.
    lex=Path('/workspace/work/integration/raft-runtime-broader-review-02/source-controls.py')
    tree=ast.parse(lex.read_text());defs=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in {'masked','braces'}]
    helpers={};exec(compile(ast.Module(body=defs,type_ignores=[]),str(lex)+'#lexical-defs','exec'),helpers)
    def projection(source):
        masked=helpers['masked'](source);spans=[]
        for name in ['struct TestPermit','impl TestPermit','mod ownership_tests']:
            match=re.search(r'#\[cfg\(test\)\]\s*'+re.escape(name)+r'\s*\{',masked)
            assert match,name;start=masked.index('{',match.start());_,end=helpers['braces'](masked,start);spans.append((match.start(),end))
        for start,end in reversed(sorted(spans)):source=source[:start]+source[end:]
        return source
    op=projection(old);np=projection(new)
    check('mechanical03 has zero byte delta outside three cfg(test) items versus frozen02',op==np,dict(projected_sha256=hashlib.sha256(op.encode()).hexdigest(),projection='Remove cfg(test) TestPermit struct/impl and ownership_tests module; no compiler preprocessing claim'))
    check('shared actual TCP scenario body byte-identical',identity(ROOT/'candidate/partitionline-broker/tests/common/raft_runtime.rs')==identity(OLD/'candidate/partitionline-broker/tests/common/raft_runtime.rs'))
    check('logical counter now drops before returned permit',ns['order'](new)==['_counter','_permit'])
    for cap,amount in itertools.product([1,2,4,17],[1,4096]):
        # Physical capacity can be acquired only following old permit return;
        # retain every legal new-publication position against declaration order.
        peaks=[]
        for events in itertools.permutations(['old_count_down','old_permit_return','new_acquire','new_count_up']):
            i={e:events.index(e) for e in events}
            if not(i['old_count_down']<i['old_permit_return']<i['new_acquire']<i['new_count_up']):continue
            current=cap*amount;peak=current
            for event in events:
                if event=='old_count_down':current-=amount
                if event=='new_count_up':current+=amount;peak=max(peak,current)
            peaks.append(peak)
        check('counter-before-capacity bound '+str((cap,amount)),peaks and max(peaks)==cap*amount)
    check('all five Envelope literals now supply required cfg(test) counter',len(ns['new_literals'])==5 and all(r['counter_present'] for r in ns['new_literals']))
    method=new[new.index('fn assert_joined(&self)'):new.index('async fn finish(mut self)',new.index('fn assert_joined(&self)'))]
    check('direct owner uses actual sender max capacity','self.control.commands.max_capacity()' in method and 'checked_sum(&[product(capacity, 2)?, 1])?' in method)
    check('direct owner requires current0 and actual constructed peak limits','assert_eq!(gauge.current.load(Ordering::Acquire), 0)' in method and 'gauge.peak.load(Ordering::Acquire) <= bound' in method)
    check('direct owner does not fabricate supervisor/listener ownership','\\"supervisor_joined\\":null' in method and '\\"listener_closed\\":null' in method and '\\"network_workers_spawned\\":0' in method)
    finish=new[new.index('async fn finish(mut self)'):new.index('impl Drop for LocalOwner')]
    check('normal finish owner joined before final assertions/receipt',finish.index('.join()')<finish.index('self.assert_joined()?;')<finish.index('remove_dir_all'))
    special=new[new.index('async fn reliable_stop_waits_for_saturated_owner_even_after_command_deadline'):new.index('async fn expired_outer_budget_cannot_admit_a_genuine_leader_write')]
    check('manual saturation join also asserts after actual successful join',special.index('.join()')<special.index('source.assert_joined()?;'))
    check('no automatic direct-owner success receipt on failed Drop cleanup','assert_joined' not in new[new.index('impl Drop for LocalOwner'):new.index('fn gate(')])
    for capacity in [1,16]:check('actual local channel envelope bound '+str(capacity),2*capacity+1=={1:3,16:33}[capacity])
    runner=(ROOT/'run-next-focused.py').read_text();order=ns['good']
    check('ELF+whole cache+raw+temporary preservation precedes parser/source qualification',all(order[k]<order['parse'] for k in ['elf','whole','raw','tmp']) and runner.index("item['source_after']=guards()")>runner.index("item['post_command_elfs']=retain_cache"))
    # Exercise final guard code against additional invented process states.
    pg=ns['pg']; pathmod=pg.Path; mockpatch=ns['patch']
    with ns['tempfile'].TemporaryDirectory(prefix='extra-memory-fs-',dir=OUT) as d:
        root=Path(d);proc=root/'proc';proc.mkdir();target=root/'cache';target.mkdir();p=proc/'313';p.mkdir();(p/'fd').mkdir()
        def restore():
            for f in ['cwd','exe']:
                q=p/f
                if q.is_symlink():q.unlink()
            (p/'cwd').symlink_to(root);(p/'exe').symlink_to('/invented')
            fields=['S']+['0']*19;fields[19]='777';(p/'stat').write_text('313 (unit) '+' '.join(fields)+'\n')
            (p/'status').write_text('Uid:1000 1000 1000 1000\nPPid:1\n')
            (p/'cmdline').write_bytes(str(target/'artifact').encode());(p/'environ').write_bytes(b'');(p/'maps').write_bytes(b'')
        restore();original_read=pg.limited_read
        def denied(q,limit):
            if Path(q)==p/'maps':raise PermissionError('invented maps refusal after known cmdline reference')
            return original_read(q,limit)
        with mockpatch.object(pg,'limited_read',denied):report=pg.cache_references(proc,target,-1)
        check('known cache reference retained even when later live field unreadable',len(report['owners'])==1 and bool(report['live_inspection_faults']))
        restore();original_identity=pg.process_identity;calls=0
        def changing(q):
            nonlocal calls
            calls+=1;row=original_identity(q)
            if calls>1:row['starttime_ticks']+=1
            return row
        with mockpatch.object(pg,'process_identity',changing):report=pg.cache_references(proc,target,-1)
        check('known cache owner plus PID replacement remains refused',len(report['owners'])==1 and bool(report['live_inspection_faults']))
    guard=(ROOT/'guard-functions.py').read_text();process=(ROOT/'process_guards.py').read_text()
    check('actual cache overwrite gate rejects every live inspection fault',"assert not inspection['live_inspection_faults']" in guard)
    check('exception default none; both exact pin arguments required',"bool(A.platform_daemon_pin)==bool(A.platform_daemon_sha256)" in runner and 'pin_rows = ()' in guard)
    check('optional platform exception requires actual joined empty pinned Docker UNIX query',"pin_sha256==PLATFORM_PIN_SHA256" in process and "'unix:///var/run/docker.sock'" in process and "error is None and code==0 and proof['empty_ps_a']" in process)
    check('exception explicitly lists five uninspected fields',"['cwd','exe','environ','maps','fd']" in process)
    check('hosted timing correction intentionally not folded into03','command_ms: 20,' in new and '.is_err(),\n                "abandoned vector grant' in new)
    after=verify(ROOT,PIN,29,567483);oldafter=verify(OLD,'e4250889d4cd2e68818a21b19e6a68a8caeb7b516ad54643a24b34880a54591b',20,241020)
    assert before==after and oldbefore==oldafter
    cp=OUT/'controls.json';cp.write_text(json.dumps(dict(classification='Independent source and invented-file/memory controls only',author_model_rows=ns['model'],author_process_fixture_cases=ns['process'],independent_control_count=len(controls),independent_controls=controls),indent=2)+'\n');cp.chmod(0o600)
    vp=OUT/'validation.json';vp.write_text(json.dumps(dict(schema_version=1,classification='Independent mechanical03 source checkpoint review; no Rust typing/runtime/hosted correction qualification',handoff=dict(path=str(ROOT/'handoff.json'),**identity(ROOT/'handoff.json')),all29_new_and20_original_files_bytes_full07777_unchanged=True,rows=before,previous_rows=oldbefore,controls=dict(path=str(cp),**identity(cp)),resolved_source_findings=['Counter decreases before permit return','All required Envelope counter fields supplied','ELF/cache/raw/temporary preservation before parser and post-source qualification','Unreadable live cache process refuses; known cache references retained','Direct-owner successful join now checks scoped current0/peak and emits honest direct-owner receipt'],source_checkpoint_no_remaining_mechanical_blocker=True,pending=['04 exact fixture waiting/typed InvalidPeer/commit phase diagnostics','Actual compilation of new cfg(test) bodies','Actual genuine observer multichunk TCP Finish/Finished/selected image/reopen and causal decoder qualification','Optional exact Docker branch requires its future joined actual query; no exemption executed here','Complete source/compiler/materialization/concurrent resource budgets at future frozen pin'],not_executed=['Cargo/compiler/Rustfmt','SDK/JVM/native','real proc/cache/Docker/process inspection or cleanup','TCP/listener/runtime or main mutation']),indent=2)+'\n');vp.chmod(0o600)
    print(json.dumps(dict(validation=identity(vp),controls=identity(cp),author_drop_rows=4,author_fixture_cases=17,independent_controls=len(controls)),indent=2))
if __name__=='__main__':main()

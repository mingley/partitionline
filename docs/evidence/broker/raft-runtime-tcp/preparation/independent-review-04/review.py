"""Independent frozen hosted-source correction review; zero runtime/compiler operations."""
import ast,hashlib,json,os,re,stat
from pathlib import Path
ROOT=Path('/workspace/work/raft-runtime-76/tcp-qualification-04')
OLD=ROOT.with_name('tcp-qualification-03')
OUT=Path(__file__).parent
PIN='01dada13e68830986afb4f468782668ae6b2816a766045a9d0f70d0b2bb06a41'
def identity(p):
    assert p.is_file() and not p.is_symlink();s=p.stat()
    return dict(sha256=hashlib.sha256(p.read_bytes()).hexdigest(),bytes=s.st_size,full07777=stat.S_IMODE(s.st_mode))
def verify(root,pin,count,total):
    assert identity(root/'handoff.json')['sha256']==pin
    m=json.loads((root/'handoff.json').read_text());assert len(m['rows'])==count and sum(r['bytes'] for r in m['rows'])==total
    result={}
    for r in m['rows']:
        p=Path(r['source']);assert p==root/r['path']
        actual=identity(p);assert actual=={k:r[k] for k in actual};result[r['path']]=actual
    return result
def main():
    os.sched_setaffinity(0,{2,4});os.umask(0o077)
    before=verify(ROOT,PIN,13,196293);prior=verify(OLD,'c745ffa88433439b7e3d785be8c64a0d81ec936e413c029f94160f34a98507cf',29,567483)
    # Replay the author's source checks up to, but excluding, its frozen receipt
    # write. This only reads declared source and compares strings/identities.
    p=ROOT/'check-source.py';text=p.read_text().split("(R/'source-controls.json').write_text",1)[0]
    ns={'__file__':str(p),'__name__':'source_checks_only'};exec(compile(text,str(p)+'#without-write','exec'),ns)
    new=(ROOT/'candidate/partitionline-broker/src/raft/runtime.rs').read_text();old=(OLD/'candidate/partitionline-broker/src/raft/runtime.rs').read_text()
    shared=(ROOT/'candidate/partitionline-broker/tests/common/raft_runtime.rs').read_text();oldshared=(OLD/'candidate/partitionline-broker/tests/common/raft_runtime.rs').read_text()
    controls=[]
    def check(name,value,details=None):
        assert value,name;controls.append(dict(name=name,passed=True,details=details))
    marker='#[cfg(test)]\nmod ownership_tests {'
    check('all runtime source before test-only ownership module byte-identical to03',new.split(marker)[0]==old.split(marker)[0])
    fixture=new[new.index('async fn fixture('):new.index('async fn state(&self)',new.index('async fn fixture('))]
    check('fixture is separately absolute bounded reliable actor wait','let deadline = Instant::now() + Duration::from_secs(2);' in fixture and 'tokio::time::timeout_at(deadline, self.control.reliable(command)).await' in fixture and 'fixture phase {phase}: absolute2s wait expired' in fixture)
    # Typed result selection semantic counterexamples. Every outcome formerly
    # accepted by is_err is now refused except the exact domain rejection.
    exact=lambda result: result==('Err','Node','InvalidPeer')
    for result in [('Err','Deadline'),('Err','Closed'),('Err','Peer'),('Err','Node','Corrupt'),('Err','Node','Poisoned'),('Ok','Done')]:
        check('exact rejection refuses '+repr(result),not exact(result))
    check('exact intended domain rejection accepted',exact(('Err','Node','InvalidPeer')))
    reject=new[new.index('async fn reject_vote('):new.index('fn assert_joined(',new.index('async fn reject_vote('))]
    check('exact runtime NodeInvalidPeer pattern and all other results fail','Err(Error::Node(replication::Error::InvalidPeer)) => {}' in reject and 'Err(other) =>' in reject and 'Ok(_) =>' in reject)
    fields=['election.persistent','election.role','base_position','last_position','committed_end','active_term','wal_durable_ops','election_durable_states','ready','poisoned']
    for field in fields:check('rejection unchanged '+field,'after.'+field in reject and 'before.'+field in reject)
    abandonment=new[new.index('async fn abandoned_vote_vector_and_full_route_release_every_exact_correlation'):new.index('async fn reliable_stop_waits_for_saturated_owner_even_after_command_deadline')]
    check('both vector/full-route checks use exact reject helper, no broad is_err',abandonment.count('.reject_vote(')==2 and '.is_err()' not in abandonment)
    check('production command20ms setting retained','command_ms: 20,' in new and old.count('command_ms: 20,')==new.count('command_ms: 20,'))
    for name,end in [('async fn reliable_stop_waits_for_saturated_owner_even_after_command_deadline','async fn expired_outer_budget_cannot_admit_a_genuine_leader_write'),('async fn expired_outer_budget_cannot_admit_a_genuine_leader_write','async fn')]:
        start=old.index(name);nstart=new.index(name)
        if name.startswith('async fn reliable'):
            ob=old[start:old.index(end,start+10)];nb=new[nstart:new.index(end,nstart+10)]
        else:
            ob=old[start:];nb=new[nstart:]
        check('intentional expiry/Stop test body unchanged '+name,ob==nb)
    commit=shared[shared.index('async fn commit('):shared.index('async fn crash(',shared.index('async fn commit('))]
    oldcommit=oldshared[oldshared.index('async fn commit('):oldshared.index('async fn crash(',oldshared.index('async fn commit('))]
    check('commit retains absolute15s and original success/prefix predicate','let deadline = Instant::now() + Duration::from_secs(15);' in commit and commit.split('let deadline',1)[1].split('if Instant::now() >= deadline',1)[0]==oldcommit.split('let deadline',1)[1].split('if Instant::now() >= deadline',1)[0])
    labels=re.findall(r'\.commit\(\s*"([a-z-]+)"',shared)
    check('all11 commit sites individually phase labeled',len(labels)==len(set(labels))==11,labels)
    check('all prior15s helper values unchanged',shared.count('Duration::from_secs(15)')==oldshared.count('Duration::from_secs(15)')==4)
    diag=shared[shared.index('fn commit_diagnostics('):shared.index('async fn commit(')]
    check('diagnostics retain actual child status and control sequence','process.child.try_wait()' in diag and 'process.next' in diag)
    check('state read bounded before firstline projection','file.take(2048).read_to_end(&mut bytes)' in diag and '.lines().next().unwrap_or("")' in diag)
    check('error diagnostics output ceiling uses UTF8-safe truncation','if text.len() > 16 * 1024' in diag and '!text.is_char_boundary(end)' in diag and 'text.truncate(end)' in diag)
    # Pure text truncation boundary model; not a Rust diagnostic/runtime run.
    for line in ['ascii'*10000,'\u03bb'*20000,'\ufffd'*20000,'\\x00'*10000]:
        raw=line.encode();suffix=b' [diagnostic truncated]';end=16384-len(suffix)
        while end and (raw[end]&0xc0)==0x80:end-=1
        projected=raw[:end]+suffix;projected.decode()
        check('bounded valid UTF8 diagnostic model '+repr(line[:5]),len(projected)<=16384)
    after=verify(ROOT,PIN,13,196293);pafter=verify(OLD,'c745ffa88433439b7e3d785be8c64a0d81ec936e413c029f94160f34a98507cf',29,567483)
    assert before==after and prior==pafter
    cp=OUT/'controls.json';cp.write_text(json.dumps(dict(classification='Source and memory controls only; zero Cargo/runtime/process operations',count=len(controls),controls=controls),indent=2)+'\n');cp.chmod(0o600)
    receipt=dict(schema_version=1,classification='Independent source-only04 install review; actual hosted failures remain unresolved observations',handoff=dict(path=str(ROOT/'handoff.json'),**identity(ROOT/'handoff.json')),all13_current_and29_previous_identities_bytes_full07777_unchanged=True,rows=before,controls=dict(path=str(cp),**identity(cp)),no_remaining_source_install_blocker=True,candidate_paths={'partitionline-broker/src/raft/runtime.rs':identity(ROOT/'candidate/partitionline-broker/src/raft/runtime.rs'),'partitionline-broker/tests/common/raft_runtime.rs':identity(ROOT/'candidate/partitionline-broker/tests/common/raft_runtime.rs')},accepted_source_changes=['Bounded absolute2s fixture observation/setup through reliable actor admission','Both genuine stale/full-route grants require exact NodeInvalidPeer and unchanged confirmed/durable fields','Production/prior cfg helpers unchanged from03; intentional20ms expiry/Stop bodies unchanged','All11 commit sites phase labeled, original15s/success semantics unchanged, child+bounded state error diagnostics only on timeout'],limitations=['No Rust typechecking, actor runtime, hosted failure repair or full76 qualification observed','2s fixture is a test-only wait; reliable owner cleanup remains retained after canceled await','Diagnostic state is current firstline/process observation, not a full synchronized WAL/image proof or identified historic failure cause','Diagnostic output capped16KiB; transient string/2048B read scratch is separate bounded test allocation','Complete immutable checkpoint/compiler/source and fresh disk/cache guards required before actual runs'])
    vp=OUT/'validation.json';vp.write_text(json.dumps(receipt,indent=2)+'\n');vp.chmod(0o600)
    print(json.dumps(dict(validation=identity(vp),controls=identity(cp),count=len(controls)),indent=2))
if __name__=='__main__':main()

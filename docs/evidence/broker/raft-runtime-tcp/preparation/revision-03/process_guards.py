"""Future fail-closed Linux cache inspection; import performs no inspection."""
import hashlib,json,os,selectors,stat,subprocess,time
from pathlib import Path
PLATFORM_PIN_SHA256='5b225336055754c792fecb6f2ce24364a6330fb93f728011ebb78ab53bd51ebd'

def limited_read(path, limit):
    with Path(path).open('rb') as source:
        value=source.read(limit+1)
    if len(value)>limit:
        raise ValueError(('process field exceeds explicit inspection bound',str(path),limit))
    return value

def process_identity(path):
    raw=limited_read(Path(path)/'stat',32768).decode();fields=raw[raw.rfind(')')+2:].split()
    return {'pid':int(Path(path).name),'comm':raw[raw.find('(')+1:raw.rfind(')')],
            'state':fields[0],'starttime_ticks':int(fields[19])}

def daemon_identities(proc, pins):
    actual=[]
    for pin in pins:
        path=Path(proc)/str(pin['pid']);a=process_identity(path)
        status=dict(r.split(':',1) for r in limited_read(path/'status',65536).decode().splitlines() if ':' in r)
        command=limited_read(path/'cmdline',1024*1024)
        row={k:a[k] for k in ('pid','comm','starttime_ticks')}
        row.update(parent_pid=int(status['PPid']),uid_all_four=[int(x) for x in status['Uid'].split()],cmdline_sha256=hashlib.sha256(command).hexdigest(),cmdline_bytes=len(command))
        assert a['state']!='Z' and all(row[k]==pin[k] for k in row),('exact platform daemon identity differs',row)
        actual.append(row)
    return actual

def cache_references(proc, target, own_pid, daemon_pins=()):
    """Unreadability of an extant nonzombie process is a refusal, never absence."""
    proc=Path(proc);target=str(Path(target).resolve());needle=target.encode();owners=[];faults=[];checked=0;zombies=0;gone=0
    exempt=daemon_identities(proc,daemon_pins);exempt_ids={r['pid'] for r in exempt}
    for p in sorted(proc.iterdir(),key=lambda x:x.name):
        if not p.name.isdigit() or int(p.name)==own_pid:continue
        try:before=process_identity(p)
        except (FileNotFoundError,ProcessLookupError) as e:
            if p.exists():faults.append({'pid':int(p.name),'field':'stat','error':repr(e)})
            else:gone+=1
            continue
        except (OSError,ValueError,UnicodeError) as e:
            faults.append({'pid':int(p.name),'field':'stat','error':repr(e)});continue
        if before['state']=='Z':zombies+=1;continue
        if int(p.name) in exempt_ids:continue
        checked+=1;refs=[]
        def fault(field,error):
            try:now=process_identity(p)
            except (FileNotFoundError,ProcessLookupError):
                if p.exists():faults.append({'pid':int(p.name),'field':field,'error':repr(error)})
                return
            except (OSError,ValueError,UnicodeError) as e:
                faults.append({'pid':int(p.name),'field':field,'error':repr(error),'liveness_error':repr(e)});return
            if now['state']!='Z':faults.append({'pid':int(p.name),'field':field,'error':repr(error)})
        for field in ('cwd','exe'):
            try:
                value=os.readlink(p/field).removesuffix(' (deleted)')
                if value==target or value.startswith(target+'/'):refs.append({'field':field,'path':value})
            except OSError as e:fault(field,e)
        for field,limit in (('status',65536),('cmdline',1024*1024),('environ',8*1024*1024),('maps',32*1024*1024)):
            try:
                raw=limited_read(p/field,limit)
                if needle in raw:refs.append({'field':field,'cache_root_present':True})
            except (OSError,ValueError) as e:fault(field,e)
        try:
            fds=list((p/'fd').iterdir())
            if len(fds)>8192:raise ValueError(('process FD inspection bound exceeded',len(fds)))
        except (OSError,ValueError) as e:fault('fd',e);fds=[]
        for fd in fds:
            try:
                value=os.readlink(fd).removesuffix(' (deleted)')
                if value==target or value.startswith(target+'/'):refs.append({'field':'fd:'+fd.name,'path':value})
            except (FileNotFoundError,ProcessLookupError) as e:
                # A descriptor genuinely closed during enumeration is absent.
                if fd.exists() or fd.is_symlink():fault('fd:'+fd.name,e)
            except OSError as e:fault('fd:'+fd.name,e)
        try:
            after=process_identity(p)
            if after['state']!='Z' and (after['comm'],after['starttime_ticks'])!=(before['comm'],before['starttime_ticks']):
                faults.append({'pid':int(p.name),'field':'identity','error':'PID identity changed during inspection'})
        except (FileNotFoundError,ProcessLookupError) as e:
            if p.exists():faults.append({'pid':int(p.name),'field':'stat-after','error':repr(e)})
        except (OSError,ValueError,UnicodeError) as e:faults.append({'pid':int(p.name),'field':'stat-after','error':repr(e)})
        if refs:owners.append({'pid':int(p.name),'process_identity':before,'cache_references':refs})
    assert daemon_identities(proc,daemon_pins)==exempt
    return {'owners':owners,'live_inspection_faults':faults,'readable_live_checked':checked,'zombies_skipped':zombies,'gone_at_initial_scan':gone,'exact_exempt_identities':exempt,'scope':'readable visible Linux processes; only supplied verified exact daemon fields are exempt, not universally inspected'}

def bounded_docker_empty(pin_path, pin_sha256, out, ordinal, proc=Path('/proc')):
    """Optional exact reviewed exception requires joined empty actual UNIX API."""
    raw=Path(pin_path).read_bytes();assert pin_sha256==PLATFORM_PIN_SHA256 and hashlib.sha256(raw).hexdigest()==pin_sha256
    pin=json.loads(raw);assert {r['pid'] for r in pin['platform_daemons']}=={199,251}
    before=daemon_identities(proc,pin['platform_daemons']);cli=pin['docker_cli'];path=Path(cli['path']);s=path.lstat()
    assert stat.S_ISREG(s.st_mode) and not path.is_symlink() and s.st_size==cli['bytes'] and s.st_mode&0o7777==cli['full_mode']
    with path.open('rb') as f:
        h=hashlib.sha256()
        while b:=f.read(1024*1024):h.update(b)
    assert h.hexdigest()==cli['sha256'];argv=[str(path),'--host','unix:///var/run/docker.sock','ps','-a','--no-trunc','--format','{{.ID}} {{.Names}} {{.Status}}']
    child=subprocess.Popen(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
    selector=selectors.DefaultSelector();selector.register(child.stdout,selectors.EVENT_READ,'stdout');selector.register(child.stderr,selectors.EVENT_READ,'stderr');buffers={'stdout':bytearray(),'stderr':bytearray()};start=time.monotonic();error=None
    try:
        while selector.get_map():
            if time.monotonic()-start>10:raise TimeoutError('joined Docker query deadline')
            for key,_ in selector.select(0.05):
                data=os.read(key.fileobj.fileno(),4096)
                if not data:selector.unregister(key.fileobj);continue
                buffers[key.data].extend(data)
                if sum(len(b) for b in buffers.values())>1024*1024:raise ValueError('bounded Docker query output exceeded')
        code=child.wait(timeout=max(0.01,10-(time.monotonic()-start)))
    except BaseException as e:
        error=repr(e);child.kill();child.wait();code=child.returncode
    finally:
        selector.close();child.stdout.close();child.stderr.close()
    after=daemon_identities(proc,pin['platform_daemons']);files={}
    for name,value in buffers.items():
        target=Path(out)/f'cache-docker-{ordinal}-{name}.log';target.write_bytes(value);files[name]={'path':str(target),'bytes':len(value),'sha256':hashlib.sha256(value).hexdigest(),'full07777':target.stat().st_mode&0o7777}
    proof={'argv':argv,'exit_code':code,'joined':True,'empty_ps_a':not buffers['stdout'].strip(),'error':error,'logs':files,'exact_daemon_identities':after,'uninspected_daemon_fields':['cwd','exe','environ','maps','fd'],'pin_sha256':pin_sha256}
    assert before==after and error is None and code==0 and proof['empty_ps_a'],proof
    return pin['platform_daemons'],proof

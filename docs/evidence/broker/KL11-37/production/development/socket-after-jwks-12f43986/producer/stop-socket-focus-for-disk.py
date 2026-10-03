import hashlib,json,os,signal,subprocess,time
from pathlib import Path
repo=Path('/workspace/partitionline');out=repo/'docs/evidence/broker/KL11-37/production/development/socket-after-jwks-12f43986';target=Path('/workspace/work/broker-oidc/target');driver_path=b'/workspace/work/broker-oidc/run-socket-focus.py';drivers=[]
for proc in Path('/proc').iterdir():
    if not proc.name.isdigit():continue
    try:args=(proc/'cmdline').read_bytes().split(b'\0')
    except OSError:continue
    if len(args)>1 and args[1]==driver_path:drivers.append(int(proc.name))
assert len(drivers)==1;driver=drivers[0];os.kill(driver,signal.SIGSTOP)
children=[]
for proc in Path('/proc').iterdir():
    if not proc.name.isdigit():continue
    try:
        fields=(proc/'stat').read_text().rsplit(')',1)[1].split();ppid=int(fields[1]);args=(proc/'cmdline').read_bytes().replace(b'\0',b' ').decode()
    except OSError:continue
    if ppid==driver:children.append(dict(pid=int(proc.name),pgid=os.getpgid(int(proc.name)),argv=args))
assert len(children)<=1
free=os.statvfs('/workspace').f_bavail*os.statvfs('/workspace').f_frsize;commands=json.loads((out/'commands.json').read_text())['commands'];latest='stable-sasl-tls-tests';log=out/(latest+'.log');receipt=dict(reason='shared filesystem headroom guard; coordinator-directed interruption and cache reclamation',base_source_sha='12f4398662044947ae653f07923290127457f2ef',driver_pid=driver,children=children,current_command=latest,completed_commands=len(commands),free_bytes_at_stop=free,target_du_bytes=int(subprocess.check_output(['du','-sb',str(target)]).split()[0]),monitor_event='HOLD stable-sasl-tls-tests process group paused: disk below350MiB',monitor_event_source='actual runner output from exec session98473',minimum_free_scope='full historical minimum was not persisted by first runner; peer/root362983424 and worker362938368 observations prove threshold breach, current stop sample is explicit',observed_free_samples=[362983424,362938368,free],interrupted_log=log.name,interrupted_log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),classification='compile/resource interruption; no failing test assertion and no product qualification for this command')
(out/'resource-interruption-01.json').write_text(json.dumps(receipt,indent=2)+'\n')
for child in children:
    os.killpg(child['pgid'],signal.SIGTERM);os.killpg(child['pgid'],signal.SIGCONT)
os.kill(driver,signal.SIGTERM);os.kill(driver,signal.SIGCONT)
time.sleep(1)
for child in children:
    try:os.killpg(child['pgid'],signal.SIGKILL)
    except ProcessLookupError:pass
print(json.dumps(receipt,indent=2))

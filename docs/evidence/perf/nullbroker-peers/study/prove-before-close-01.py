import pathlib,subprocess,socket,time,struct,json,sys,os,signal,hashlib
w=pathlib.Path(__file__).parent;out=w/'before-close-01';out.mkdir(exist_ok=False);wrapper='/workspace/partitionline/benchmarks/runtime/tools/parent-bound-exec.py';exe=w/'before-build-01/nullbroker.elf';assert hashlib.sha256(exe.read_bytes()).hexdigest()==json.load(open(w/'before-build-01/binary-binding.json'))['binary_sha256']
with socket.socket() as s:s.bind(('127.0.0.1',0));port=s.getsockname()[1]
cmd=[str(exe),'--bind',f'127.0.0.1:{port}','--seconds','2','--artifact',str(out/'broker.json')];observed={}
with (out/'broker.log').open('wb') as log:
 p=subprocess.Popen([sys.executable,wrapper,str(os.getpid()),*cmd],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 try:
  deadline=time.monotonic()+1
  while True:
   try:s=socket.create_connection(('127.0.0.1',port),.1);break
   except OSError:
    if p.poll() is not None or time.monotonic()>deadline:raise RuntimeError('not ready')
    time.sleep(.01)
  with s:
   s.settimeout(.2)
   body=b'\x00'+struct.pack('>hi',-1,1000)+b'\x01\x00';header=struct.pack('>hhih',0,13,1,1)+b'p\x00';frame=header+body;s.sendall(struct.pack('>i',len(frame))+frame)
   try:observed['read']=s.recv(1).hex();observed['premature_eof']=observed['read']==''
   except TimeoutError:observed['read_timeout_before_shutdown']=True
  rc=p.wait(timeout=4)
 finally:
  if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=2)
with socket.socket() as s:s.bind(('127.0.0.1',port))
try:os.killpg(p.pid,0);empty=False
except ProcessLookupError:empty=True
receipt=dict(command=cmd,pid=p.pid,exit=rc,joined=True,process_group_empty=empty,port_rebound=True,observed=observed,source_commit='18e12bda8350d7631aeef2af8c19a6a38d85c4dc',binary_sha256=hashlib.sha256(exe.read_bytes()).hexdigest())
(out/'receipts.json').write_text(json.dumps(receipt,indent=2)+'\n');assert observed.get('read_timeout_before_shutdown') and rc==0 and empty;print(json.dumps(receipt))

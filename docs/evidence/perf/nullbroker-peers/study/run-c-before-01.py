import subprocess,socket,time,json,hashlib,pathlib,os
root=pathlib.Path(__file__).parent; out=root/'before-c-01'; out.mkdir(exist_ok=False)
with socket.socket() as s: s.bind(('127.0.0.1',0)); port=s.getsockname()[1]
address=f'127.0.0.1:{port}'
commands=[[str(root/'before-build-01/nullbroker.elf'),'--bind',address,'--partitions','1','--seconds','10','--artifact',str(out/'broker.json'),'--fetch-records','512','--fetch-batch-records','128','--fetch-payload-bytes','100'],[str(root/'c-peer.elf'),address]]
children=[]; receipts=[]
try:
 with open(out/'broker.log','wb') as log:
  b=subprocess.Popen(commands[0],stdout=log,stderr=subprocess.STDOUT,start_new_session=True); children.append(b)
  ready=time.monotonic()+3
  while True:
   try:
    with socket.create_connection(('127.0.0.1',port),.1): break
   except OSError:
    if b.poll() is not None or time.monotonic()>ready: raise RuntimeError('broker not ready')
    time.sleep(.01)
  with open(out/'peer.json','wb') as stdout,open(out/'peer.log','wb') as stderr:
   p=subprocess.Popen(commands[1],stdout=stdout,stderr=stderr,start_new_session=True); children.append(p)
   rc=p.wait(timeout=15); receipts.append({'command':commands[1],'pid':p.pid,'exit':rc,'joined':True})
  rc=b.wait(timeout=12); receipts.append({'command':commands[0],'pid':b.pid,'exit':rc,'joined':True})
 with socket.socket() as s: s.bind(('127.0.0.1',port))
 receipts.append({'port':port,'rebound':True})
finally:
 for p in children:
  if p.poll() is None: os.killpg(p.pid,9); p.wait(timeout=3)
 (out/'receipts.json').write_text(json.dumps(receipts,indent=2)+'\n')
print(json.dumps(receipts))

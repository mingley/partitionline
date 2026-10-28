#!/usr/bin/env python3
"""Run an independent Java Admin history against three local metadata controllers."""
import argparse, hashlib, json, os, select, signal, socket, struct, subprocess, time
from pathlib import Path

ROOT=Path('/workspace/work/quorum-java-peer')
JARS=Path('/workspace/work/security-qualification-20261028/java-peer/jars')
WORK=Path('/workspace/work/partitionline-quorum')
INPUTS=['partitionline-broker/src/metadata.rs','partitionline-broker/src/bin/operator.rs',
        'partitionline-broker/src/bin/operator/serve.rs','partitionline-broker/src/bin/operator/quorum.rs',
        'partitionline-broker/Cargo.toml','partitionline-broker/Cargo.lock']
def digest(p):
 with p.open('rb') as f: return hashlib.file_digest(f,'sha256').hexdigest()
def sources(): return {name:digest(WORK/name) for name in INPUTS}
def write(path,data): path.write_text(json.dumps(data,indent=2,sort_keys=True)+'\n')
def metadata(port):
 payload=struct.pack('>hhi h i',3,1,77,-1,-1)
 with socket.create_connection(('127.0.0.1',port),2) as s:
  s.settimeout(3);s.sendall(struct.pack('>i',len(payload))+payload)
  def exact(n):
   result=b''
   while len(result)<n:
    b=s.recv(n-len(result))
    if not b: raise ValueError('closed')
    result+=b
   return result
  n=struct.unpack('>i',exact(4))[0]
  if not 0<n<=1048576: raise ValueError('invalid metadata frame')
  raw=exact(n)
 at=0
 def i32():
  nonlocal at
  if at+4>len(raw): raise ValueError('truncated i32')
  v=struct.unpack_from('>i',raw,at)[0];at+=4;return v
 def string():
  nonlocal at
  if at+2>len(raw): raise ValueError('truncated string')
  n=struct.unpack_from('>h',raw,at)[0];at+=2
  if n==-1:return None
  if n<0 or at+n>len(raw):raise ValueError('invalid string')
  v=raw[at:at+n].decode('utf-8');at+=n;return v
 if i32()!=77:raise ValueError('correlation mismatch')
 count=i32()
 if count!=3:raise ValueError(f'expected three endpoints, got {count}')
 brokers=[]
 for _ in range(count): brokers.append({'id':i32(),'host':string(),'port':i32(),'rack':string()})
 controller=i32()
 return {'controller':controller,'brokers':brokers,'response_sha256':hashlib.sha256(raw).hexdigest(),'response_hex':raw.hex()}
def wait_leader(ports,alive,history,excluded=None):
 deadline=time.monotonic()+20
 while time.monotonic()<deadline:
  for node in sorted(alive):
   try:
    result=metadata(ports[node]); result['queried_node']=node
    if len(history)<256: history.append(result)
    leader=result['controller']; expected=[{'id':i,'host':'127.0.0.1','port':ports[i],'rack':None} for i in range(3)]
    if result['brokers']!=expected: raise ValueError('advertised endpoints differ')
    if leader in alive and leader!=excluded: return leader
   except (OSError,TimeoutError): pass
  time.sleep(.05)
 raise TimeoutError('leader discovery')
def start(id,roots,members,operator,output):
 stdout=(output/f'server-{id}-{len(list(output.glob(f"server-{id}-*.stdout")))}.stdout').open('xb')
 stderr=(output/f'server-{id}-{len(list(output.glob(f"server-{id}-*.stderr")))}.stderr').open('xb')
 args=[str(operator),'serve-quorum','--data',str(roots[id]),'--cluster','java-quorum-test','--id',str(id),'--members',members]
 proc=subprocess.Popen(args,stdout=stdout,stderr=stderr,start_new_session=True);deadline=time.monotonic()+20
 try:
  while time.monotonic()<deadline:
   stdout.flush()
   data=Path(stdout.name).read_text(errors='replace')
   if 'profile=metadata-only-quorum' in data:return proc,stdout,stderr,args
   if proc.poll() is not None:raise RuntimeError(f'server {id} exited {proc.returncode}: {Path(stderr.name).read_text(errors="replace")}')
   time.sleep(.05)
  raise TimeoutError(f'server {id} startup')
 except Exception:
  if proc.poll() is None:
   proc.terminate()
   try:proc.wait(timeout=5)
   except subprocess.TimeoutExpired:proc.kill();proc.wait()
  stdout.close();stderr.close();raise
def shutdown(entry):
 p,stdout,stderr,_=entry
 if p.poll() is None:p.send_signal(signal.SIGTERM)
 try: result=p.wait(timeout=15)
 except subprocess.TimeoutExpired: p.kill();p.wait();raise
 finally: stdout.close();stderr.close()
 if result!=0:raise RuntimeError(f'server shutdown returned {result}')
def java(action,topic,bootstrap,out,seq):
 cp=os.pathsep.join([str(ROOT/'classes'),str(JARS/'kafka-clients-4.1.2.jar'),str(JARS/'slf4j-api-1.7.36.jar')])
 command=['java','-cp',cp,'QuorumPeer',bootstrap,action,topic]
 result=subprocess.run(command,capture_output=True,text=True,timeout=25)
 (out/f'java-{seq}-{action}.stdout').write_text(result.stdout);(out/f'java-{seq}-{action}.stderr').write_text(result.stderr)
 if result.returncode:raise RuntimeError(f'Java {action} failed {result.returncode}; retained output')
 return {'command':command,'exit':result.returncode,'stdout_sha256':digest(out/f'java-{seq}-{action}.stdout'),'stderr_sha256':digest(out/f'java-{seq}-{action}.stderr')}
def eventually(action,topic,bootstrap,out,seq):
 last=None
 for attempt in range(3):
  try:return java(action,topic,bootstrap,out,f'{seq}-{attempt}')
  except RuntimeError as error:last=error;time.sleep(.2)
 raise RuntimeError(f'Java {action} did not converge after three retained attempts: {last}')

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--operator',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
 operator=args.operator.resolve();out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
 before=sources();op_hash=digest(operator);history=[];processes={};results=[];logs={};failure=None
 reservations=[socket.socket() for _ in range(6)]
 for s in reservations:s.bind(('127.0.0.1',0));s.listen(1)
 ports_all=[s.getsockname()[1] for s in reservations]
 for s in reservations:s.close()
 private=ports_all[::2];ports=ports_all[1::2];roots=[out/f'data-{i}' for i in range(3)]
 members=','.join(f'{i}@{i+1:032x}@127.0.0.1:{private[i]}@127.0.0.1:{ports[i]}' for i in range(3))
 try:
  for i in range(3):processes[i]=start(i,roots,members,operator,out)
  alive=set(processes);leader=wait_leader(ports,alive,history); follower=next(i for i in alive if i!=leader)
  results.append(java('create','java-before',f'127.0.0.1:{ports[follower]}',out,1))
  for i in sorted(alive):results.append(eventually('list','java-before',f'127.0.0.1:{ports[i]}',out,10+i))
  shutdown(processes.pop(leader));alive.remove(leader);next_leader=wait_leader(ports,alive,history,leader)
  follower=next(i for i in alive if i!=next_leader)
  results.append(java('create','java-after',f'127.0.0.1:{ports[follower]}',out,20))
  processes[leader]=start(leader,roots,members,operator,out);alive.add(leader)
  for i in sorted(alive):results.append(eventually('list','java-after',f'127.0.0.1:{ports[i]}',out,30+i))
  current=wait_leader(ports,alive,history);follower=next(i for i in alive if i!=current)
  results.append(java('delete','java-before',f'127.0.0.1:{ports[follower]}',out,40))
  for i in sorted(alive):results.append(eventually('absent','java-before',f'127.0.0.1:{ports[i]}',out,50+i))
 except Exception as e: failure=f'{type(e).__name__}: {e}'
 finally:
  for i,entry in list(processes.items()):
   try:shutdown(entry)
   except Exception as e: failure=(failure+'; ' if failure else '')+f'shutdown {i}: {type(e).__name__}: {e}'
 after=sources();stable=before==after and digest(operator)==op_hash
 if not stable:failure=(failure+'; ' if failure else '')+'candidate or binary changed during peer run'
 write(out/'history.json',history);write(out/'result.json',{'state':'failed' if failure else 'passed','failure':failure,'sources_before':before,'sources_after':after,'operator_sha256':op_hash,'peer_source_sha256':digest(ROOT/'QuorumPeer.java'),'java_class_sha256':digest(ROOT/'classes/QuorumPeer.class'),'jar_sha256':{p.name:digest(p) for p in [JARS/'kafka-clients-4.1.2.jar',JARS/'slf4j-api-1.7.36.jar']},'members':members,'results':results,'source_stable':stable,'scope':'One local fixed three-node private metadata-only quorum and one Java Admin; no replicated data, native KRaft, security or performance qualification'})
 print(json.dumps({'state':'failed' if failure else 'passed','failure':failure,'output':str(out)}))
 if failure:raise SystemExit(1)
if __name__=='__main__':main()

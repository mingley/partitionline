import base64, json, os, pathlib, socket, subprocess, time, uuid
root = pathlib.Path('/workspace/work/bench-history/native')
root.mkdir(exist_ok=False)
kafka = pathlib.Path('/workspace/work/c-peer/kafka_2.13-3.9.1')
sockets = [socket.socket() for _ in range(2)]
for s in sockets: s.bind(('127.0.0.1',0))
port, controller = [s.getsockname()[1] for s in sockets]
for s in sockets: s.close()
properties = root/'server.properties'
properties.write_text(f'''process.roles=broker,controller
node.id=147
controller.quorum.voters=147@127.0.0.1:{controller}
listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}
advertised.listeners=PLAINTEXT://127.0.0.1:{port}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs={root}/data
num.network.threads=2
num.io.threads=4
background.threads=2
num.partitions=2
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
''')
env=os.environ|{'KAFKA_HEAP_OPTS':'-Xms256m -Xmx256m','LOG_DIR':str(root/'logs')}
cluster=base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('=')
with (root/'format.log').open('w') as output:
 subprocess.run(['taskset','-c','0-2,4',str(kafka/'bin/kafka-storage.sh'),'format','-t',cluster,'-c',str(properties)],env=env,stdout=output,stderr=subprocess.STDOUT,check=True,timeout=45)
with (root/'server.log').open('w') as output:
 process=subprocess.Popen(['taskset','-c','0-2,4',str(kafka/'bin/kafka-server-start.sh'),str(properties)],env=env,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
(root/'pid').write_text(str(process.pid))
metadata={'bootstrap':f'127.0.0.1:{port}','controller':controller,'pid':process.pid,'cluster_id':cluster,'topic':'kl04-07-'+uuid.uuid4().hex[:10],'kafka_home':str(kafka),'config':str(properties)}
(root/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
for _ in range(50):
 if process.poll() is not None: raise RuntimeError('isolated Kafka exited')
 try:
  with socket.create_connection(('127.0.0.1',port),timeout=1): break
 except OSError: time.sleep(.5)
else: raise RuntimeError('isolated Kafka did not open its port')
with (root/'create-topic.log').open('w') as output:
 subprocess.run(['taskset','-c','0-2,4',str(kafka/'bin/kafka-topics.sh'),'--bootstrap-server',metadata['bootstrap'],'--create','--topic',metadata['topic'],'--partitions','2','--replication-factor','1'],env=env,stdout=output,stderr=subprocess.STDOUT,check=True,timeout=45)
print(json.dumps(metadata))

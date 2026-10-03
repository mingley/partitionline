import json,struct,hashlib,stat
from pathlib import Path
ROOT=Path(__file__).parent

def vi(n):
 b=bytearray()
 while n>=128:b.append((n&127)|128);n>>=7
 b.append(n);return bytes(b)

def string(s):return vi(len(s)+1)+s

class Buf:
 def __init__(self,b):self.b=b;self.p=0
 def take(self,n):
  if self.p+n>len(self.b):raise ValueError('short body')
  b=self.b[self.p:self.p+n];self.p+=n;return b
 def var(self):
  value=0
  for i in range(5):
   byte=self.take(1)[0];value|=(byte&127)<<(7*i)
   if not byte&128:return value
  raise ValueError('varint overflow')
 def array(self):
  n=self.var();n=0 if n==0 else n-1
  if n>len(self.b)-self.p:raise ValueError('array exceeds remaining bytes')
  return n
 def text(self):
  n=self.var()
  if n==0:return
  n-=1
  if n>65536:raise ValueError('string cap')
  self.take(n).decode('utf8')
 def tags(self):
  count=self.var()
  if count>len(self.b)-self.p:raise ValueError('tag count')
  for i in range(count):self.var();self.take(self.var())

def candidate_preflight(b,remaining=100000):
 c=Buf(b);c.take(4);c.take(2);unknown=c.array()
 if unknown>8192:raise ValueError('unknown count')
 for i in range(unknown):c.text()
 count=c.array()
 if count>remaining or count>(len(b)-c.p)//11:raise ValueError('listing cap')
 for i in range(count):c.text();c.take(8);c.text();c.tags()
 c.tags()
 if c.p!=len(b):raise ValueError('trailing')
 return count

H=bytes(6);entry=string(b'tx')+struct.pack('>q',7)+string(b'Ongoing')+b'\x00'
cases=[
 ('valid-empty',H+b'\x01\x01\x00',True,'valid required empty arrays'),
 ('null-unknown-array',H+b'\x00\x01\x00',True,'invalid nonnullable UnknownStateFilters'),
 ('null-listings-array',H+b'\x01\x00\x00',True,'invalid nonnullable TransactionStates'),
 ('null-unknown-string',H+b'\x02\x00\x01\x00',True,'invalid nonnullable unknown-filter element'),
 ('null-transaction-id',H+b'\x01\x02\x00'+struct.pack('>q',7)+string(b'Ongoing')+b'\x00\x00',True,'invalid nonnullable TransactionalId'),
 ('null-transaction-state',H+b'\x01\x02'+string(b'tx')+struct.pack('>q',7)+b'\x00\x00\x00',True,'invalid nonnullable TransactionState'),
 ('duplicate-top-tags',H+b'\x01\x01\x02\x03\x00\x03\x00',True,'duplicate unknown tags'),
 ('descending-top-tags',H+b'\x01\x01\x02\x05\x00\x03\x00',True,'descending unknown tags'),
 ('duplicate-entry-tags',H+b'\x01\x02'+entry[:-1]+b'\x02\x03\x00\x03\x00\x00',True,'duplicate entry unknown tags'),
 ('valid-distinct-tags',H+b'\x01\x01\x02\x03\x00\x05\x00',True,'valid strictly ascending unknown tags'),
 ('valid-listing',H+b'\x01\x02'+entry+b'\x00',True,'valid listing'),
 ('trailing-byte',H+b'\x01\x01\x00x',False,'trailing byte refusal'),
 ('unknown-count-cap',H+vi(8194)+bytes(8193)+b'\x01\x00',False,'unknown-filter cap before decode'),
 ('remaining-listings-cap',H+b'\x01\x02'+entry+b'\x00',False,'aggregate listing count at zero remainder'),
]
rows=[]
for name,b,expected,reason in cases:
 try:count=candidate_preflight(b,0 if name=='remaining-listings-cap' else 100000);accepted=True
 except ValueError as error:count=None;accepted=False
 assert accepted==expected,(name,accepted,expected)
 rows.append(dict(name=name,body_hex=b.hex() if len(b)<1024 else None,body_bytes=len(b),sha256=hashlib.sha256(b).hexdigest(),candidate_model_accepted=accepted,count=count,reason=reason))
# Metadata v9: one broker, valid fields, a malicious unexpected topics count.
# Mirrors only the preallocation reachability; never allocates a topic Vec.
prefix=bytes(4)+vi(2)+struct.pack('>i',1)+string(b'h')+struct.pack('>i',9092)+b'\x00\x00'+b'\x00'+struct.pack('>i',-1)
body=prefix+vi(500001)+bytes(500000)
c=Buf(body);c.take(4);assert c.array()==1
c.take(4);c.text();c.take(4);c.text();c.tags();c.text();c.take(4);count=c.array();assert count==500000 and len(body)<=1024*1024
result=dict(schema_version=1,scope='Python source-semantic controls only, independently specified required-field/tag policy; no Rust/Cargo/runtime execution',candidate_sha256=hashlib.sha256((ROOT/'admin.candidate.rs').read_bytes()).hexdigest(),controls=rows,metadata_preallocation_counterexample=dict(version=9,body_bytes=len(body),body_sha256=hashlib.sha256(body).hexdigest(),broker_count=1,unexpected_topic_count=count,ordinary_decoder_reaches_topic_Vec_allocation_before_first_topic_decode=True,model_allocated_topics=0),actual_Rust_runs=0,actual_operational_mutations=0)
p=ROOT/'controls.json';assert not p.exists();p.write_text(json.dumps(result,indent=2)+'\n');p.chmod(0o600);print(json.dumps({'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size,'full_mode':stat.S_IMODE(p.stat().st_mode),'controls':len(rows)}))

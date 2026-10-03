from pathlib import Path
import json,struct,hashlib,stat
R=Path(__file__).parent
old=R.parent/'controls.py'
ns={'__file__':str(old)};exec(old.read_text().split('H=bytes(6)')[0],ns)
vi=ns['vi'];string=ns['string'];OriginalBuf=ns['Buf']
class B(OriginalBuf):
 def arr(self,flex=True):
  if flex:
   n=self.var();n=None if n==0 else n-1
  else:
   n=struct.unpack('>i',self.take(4))[0];n=None if n<0 else n
  if n is not None and n>len(self.b)-self.p:raise ValueError('array exceeds remaining')
  return n
 def txt(self,flex=True,nullable=False):
  if flex:
   n=self.var()
   if n==0:
    if nullable:return
    raise ValueError('required null')
   n-=1
  else:
   n=struct.unpack('>h',self.take(2))[0]
   if n<0:
    if n==-1 and nullable:return
    raise ValueError('required null/invalid negative')
  if n>65536:raise ValueError('text cap')
  self.take(n).decode('utf8')

def listing(b,remaining=100000):
 c=B(b);c.take(6);n=c.arr()
 if n is None:raise ValueError('null unknown array')
 if n>8192:raise ValueError('unknown count')
 for _ in range(n):c.txt()
 n=c.arr()
 if n is None:raise ValueError('null listings')
 if n>remaining or n>(len(b)-c.p)//11:raise ValueError('listing cap')
 for _ in range(n):c.txt();c.take(8);c.txt();c.tags()
 c.tags()
 if c.p!=len(b):raise ValueError('trailing')
 return n

def metadata(b,v):
 c=B(b);f=v>=9
 if v>=3:c.take(4)
 n=c.arr(f)
 if n is None or not 1<=n<=256:raise ValueError('broker count')
 for _ in range(n):
  c.take(4);c.txt(f);port=struct.unpack('>i',c.take(4))[0]
  if not 1<=port<=65535:raise ValueError('port')
  if v>=1:c.txt(f,True)
  if f:c.tags()
 if v>=2:c.txt(f,True)
 if v>=1:c.take(4)
 if c.arr(f)!=0:raise ValueError('unexpected topics before decode')
 if 8<=v<=10:c.take(4)
 if v>=13:c.take(2)
 if f:c.tags()
 if c.p!=len(b):raise ValueError('trailing')
 return n

def arr(n,f):return vi(0 if n is None else n+1) if f else struct.pack('>i',-1 if n is None else n)
def txt(s,f):return (b'\x00' if s is None else string(s)) if f else (struct.pack('>h',-1) if s is None else struct.pack('>h',len(s))+s)
def md(v,topics=0,broker_null=False,host=b'h',port=9092,rack=None,cluster=None):
 f=v>=9;b=(bytes(4) if v>=3 else b'')+arr(None if broker_null else 1,f)
 if not broker_null:
  b+=struct.pack('>i',1)+txt(host,f)+struct.pack('>i',port)
  if v>=1:b+=txt(rack,f)
  if f:b+=b'\x00'
 if v>=2:b+=txt(cluster,f)
 if v>=1:b+=struct.pack('>i',-1)
 b+=arr(topics,f)
 if 8<=v<=10:b+=struct.pack('>i',-2147483648)
 if v>=13:b+=bytes(2)
 if f:b+=b'\x00'
 return b

rows=[]
def check(name,b,wanted,fn):
 try:value=fn(b);accepted=True;error=None
 except (ValueError,UnicodeDecodeError) as e:value=None;accepted=False;error=str(e)
 assert accepted==wanted,(name,accepted,wanted)
 rows.append(dict(name=name,accepted=accepted,expected=wanted,bytes=len(b),sha256=hashlib.sha256(b).hexdigest(),body_hex=b.hex() if len(b)<1024 else None,result=value,rejection=error))
oldrows=json.loads((R.parent/'controls.json').read_text())['controls']
for x in oldrows:
 if x['body_hex'] is None:continue
 valid=x['candidate_model_accepted'] and not x['name'].startswith('null-')
 if x['name']=='remaining-listings-cap':valid=False
 check('old-'+x['name'],bytes.fromhex(x['body_hex']),valid,lambda b,n=x['name']:listing(b,0 if n=='remaining-listings-cap' else 100000))
# Exact former500025B Metadata body reaches no allocating decoder after refusal.
prefix=bytes(4)+vi(2)+struct.pack('>i',1)+string(b'h')+struct.pack('>i',9092)+b'\x00\x00'+b'\x00'+struct.pack('>i',-1)
large=prefix+vi(500001)+bytes(500000);assert len(large)==500025
check('old-large-unexpected-topic-count',large,False,lambda b:metadata(b,9))
for v in range(1,14):
 for name,args in [('nullable-valid',{}),('nonnull-valid',dict(rack=b'r',cluster=b'cluster')),('null-topic-array',dict(topics=None)),('nonzero-topic-count',dict(topics=1)),('null-broker-array',dict(broker_null=True)),('null-host',dict(host=None)),('port-zero',dict(port=0)),('port-max',dict(port=65535)),('port-overflow',dict(port=65536))]:
  positive=name in ('nullable-valid','nonnull-valid','port-max')
  b=md(v,**args)
  if name=='nonzero-topic-count':b+=bytes(20)
  check(f'metadata-v{v}-{name}',b,positive,lambda b,v=v:metadata(b,v))
 check(f'metadata-v{v}-trailing',md(v)+b'x',False,lambda b,v=v:metadata(b,v))
# Classic nullable -2 is forbidden, -1/nonnull are accepted per field schema.
b=md(1);rack_at=4+4+2+1+4;b=b[:rack_at]+struct.pack('>h',-2)+b[rack_at+2:]
check('classic-invalid-negative-rack',b,False,lambda b:metadata(b,1))
source=Path('/workspace/work/list-transactions-routing-01/admin.candidate.rs');assert hashlib.sha256(source.read_bytes()).hexdigest()=='9bf5265936028337c03cbe5a3e9d314174c00c21727252e68aefc2e6eaee2a3e'
out=R/'controls.json';assert not out.exists();value=dict(schema_version=1,scope='Python source-semantic revised parser models only; no Rust/Apache execution',source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),controls=rows,total=len(rows),all_pass=True,topics_allocated_by_review=0,huge_topics_rejected_before_decoder=True,actual_Rust_runs=0)
out.write_text(json.dumps(value,indent=2)+'\n');out.chmod(0o600);print(json.dumps(dict(path=str(out),sha256=hashlib.sha256(out.read_bytes()).hexdigest(),bytes=out.stat().st_size,full_mode=stat.S_IMODE(out.stat().st_mode),controls=len(rows))))

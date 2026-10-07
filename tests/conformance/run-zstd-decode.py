#!/usr/bin/env python3
"""Generate and check bounded zstd fixtures with Apache Kafka and native libzstd."""
import argparse, hashlib, json, struct, subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
JAR_SHA='52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def run(args,**kw):return subprocess.run(args,check=True,timeout=60,**kw)
def crc32c(data):
 crc=0xffffffff
 for byte in data:
  crc^=byte
  for _ in range(8):crc=(crc>>1)^ (0x82f63b78 if crc&1 else 0)
 return crc^0xffffffff
def batch(head,frame):
 out=bytearray(head[:61]+frame);struct.pack_into('>i',out,8,len(out)-12)
 attr=struct.unpack_from('>h',out,21)[0];struct.pack_into('>h',out,21,(attr&~7)|4)
 struct.pack_into('>I',out,17,crc32c(out[21:]));return out
def header(frame):
 descriptor=frame[4];single=bool(descriptor&32);at=5;window=None
 if not single:
  w=frame[at];at+=1;base=1<<(10+(w>>3));window=base+(base>>3)*(w&7)
 dictsize=(0,1,2,4)[descriptor&3];at+=dictsize;sizebits=descriptor>>6
 sizes=(1 if single else 0,2,4,8);length=sizes[sizebits]
 size=int.from_bytes(frame[at:at+length],'little') if length else None
 if length==2:size+=256
 return dict(content_size=size,window_size=size if single else window,checksum=bool(descriptor&4),single_segment=single)
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('mode',choices=['generate','verify']);p.add_argument('--jar',type=Path,required=True);p.add_argument('--libs',type=Path,required=True);p.add_argument('--fixtures',type=Path,required=True);p.add_argument('--rust-output',type=Path);p.add_argument('--c-log',type=Path);p.add_argument('--report',type=Path,required=True);a=p.parse_args()
 if sha(a.jar)!=JAR_SHA:raise ValueError('Apache jar SHA mismatch')
 source=ROOT/'conformance/java/ZstdDecodeFixtures.java'
 java=['java','-Xmx128m','--class-path',str(a.jar)+':'+str(a.libs/'*'),str(source),str(a.jar),str(a.fixtures)]
 if a.mode=='generate':
  a.fixtures.mkdir(parents=True,exist_ok=True);run(java+['generate'])
  plain=(a.fixtures/'java-none.batch').read_bytes();payload=plain[61:];raw=a.fixtures/'records.raw';raw.write_bytes(payload)
  variants={}
  def compress(data,opts,known=True):
   if known:return subprocess.check_output(['zstd','-q','-c','--single-thread']+opts+[str(raw)],timeout=60)
   return subprocess.check_output(['zstd','-q','-c','--single-thread']+opts,input=data,timeout=60)
  for name,opts,known in [('native-level1-known',['-1'],True),('native-level19-known',['-19'],True),('native-no-check',['-3','--no-check'],True),('native-unknown-window18',['-3','--no-content-size','--zstd=wlog=18'],False),('native-unknown-window20',['-3','--no-content-size','--zstd=wlog=20'],False)]:
   variants[name]=compress(payload,opts,known)
  variants['native-concatenated']=compress(payload[:len(payload)//2],['-1'],False)+compress(payload[len(payload)//2:],['-3'],False)
  skip=struct.pack('<II',0x184d2a5f,4)+b'peer'
  variants['native-skippable']=skip+variants['native-level1-known']+skip
  variants['native-1024-frames']=struct.pack('<II',0x184d2a50,0)*1023+variants['native-level1-known']
  variants['native-window64m']=b'\x28\xb5\x2f\xfd\x00\x80'+b'\x01\x00\x00' # empty final raw block, window=64 MiB
  # The empty frame is followed by actual records. Java must agree on the sequence.
  variants['native-window64m']+=variants['native-level1-known']
  for name,frame in variants.items():(a.fixtures/(name+'.batch')).write_bytes(batch(plain,frame))
  malformed={
   'empty':b'', 'short-magic':b'\x28\xb5', 'invalid-magic':b'bad!',
   'reserved-header':b'\x28\xb5\x2f\xfd\x08\x00',
   'huge-window':b'\x28\xb5\x2f\xfd\x00\xff',
   'window-over64m':b'\x28\xb5\x2f\xfd\x00\x81\x01\x00\x00',
   'huge-content-size':b'\x28\xb5\x2f\xfd\xe0'+struct.pack('<Q',2**64-1),
   'content-over64m-small-window':b'\x28\xb5\x2f\xfd\xc0\x00'+struct.pack('<Q',64*1024*1024+1)+b'\x01\x00\x00',
   'dictionary':b'\x28\xb5\x2f\xfd\x01\x00\x01\x01\x00\x00',
   'truncated-skippable':struct.pack('<II',0x184d2a50,2**32-1),
   'too-many-frames':struct.pack('<II',0x184d2a50,0)*1025+variants['native-level1-known'],
   'truncated-frame':variants['native-level1-known'][:-1],
   'trailing-byte':variants['native-level1-known']+b'?',
   'partial-output-error':variants['native-concatenated'][:-1],
  }
  badcheck=bytearray(variants['native-level1-known']);badcheck[-1]^=1;malformed['bad-checksum']=badcheck
  for name,frame in malformed.items():(a.fixtures/(name+'.bad')).write_bytes(batch(plain,frame))
  if a.c_log:
   data=a.c_log.read_bytes();at=0;count=0
   while at<len(data):
    size=struct.unpack_from('>i',data,at+8)[0]+12;part=data[at:at+size]
    if len(part)!=size or struct.unpack_from('>h',part,21)[0]&7!=4:raise ValueError('C producer did not retain complete zstd batches')
    (a.fixtures/f'librdkafka-{count:03}.native').write_bytes(part);count+=1;at+=size
   if not count:raise ValueError('empty C topic log')
  run(java+['verify'])
  entries=[dict(path=x.name,sha256=sha(x),size_bytes=x.stat().st_size) for x in sorted(a.fixtures.iterdir()) if x.is_file() and x.name!='manifest.json']
  properties={x.name:header(x.read_bytes()[61:]) for x in a.fixtures.glob('*.batch') if x.name not in ('java-none.batch','native-skippable.batch','native-1024-frames.batch')}
  manifest=dict(schema_version=1,apache_jar_sha256=JAR_SHA,native_cli_version=subprocess.check_output(['zstd','--version'],text=True).strip(),decoded_record_section_bytes=len(payload),java_source_sha256=sha(source),generator_sha256=sha(Path(__file__)),files=entries,frame_properties=properties)
  (a.fixtures/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
 else:
  m=json.loads((a.fixtures/'manifest.json').read_text())
  if m['apache_jar_sha256']!=JAR_SHA or m['java_source_sha256']!=sha(source) or m['generator_sha256']!=sha(Path(__file__)):raise ValueError('source/peer pins changed')
  for item in m['files']:
   f=a.fixtures/item['path']
   if sha(f)!=item['sha256'] or f.stat().st_size!=item['size_bytes']:raise ValueError('fixture pin mismatch: '+item['path'])
  run(java+['verify']+([str(a.rust_output)] if a.rust_output else []))
 outputs=[]
 if a.rust_output:
  batches=sorted(a.rust_output.glob('*.batch'))
  required={f'level-{level:02}.batch' for level in range(1,20)}
  if not required.issubset({f.name for f in batches}):raise ValueError('missing explicit encoder levels')
  for f in batches:
   expected=f.with_suffix('.expected')
   outputs.append(dict(path=f.name,sha256=sha(f),expected_sha256=sha(expected),size_bytes=f.stat().st_size))
 a.report.write_text(json.dumps(dict(status='pass',mode=a.mode,manifest_sha256=sha(a.fixtures/'manifest.json'),fixture_count=len(json.loads((a.fixtures/'manifest.json').read_text())['files']),rust_outputs=outputs),indent=2)+'\n')
if __name__=='__main__':main()

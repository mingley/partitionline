#!/usr/bin/env python3
"""Compile isolated exact source snippets and compare deterministic bytes."""
import hashlib, itertools, json, os, pathlib, subprocess, sys
R=pathlib.Path('/workspace/partitionline')
D=pathlib.Path(__file__).resolve().parent

def sha_bytes(data): return hashlib.sha256(data).hexdigest()
def sha(path): return sha_bytes(path.read_bytes())
def write(name,text): (D/name).write_text(text)
def run(name,args,env=None,stdin=None):
 p=subprocess.run(args,cwd=D,env=env,input=stdin,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=60)
 write(name+'.stdout',p.stdout);write(name+'.stderr',p.stderr)
 write(name+'.json',json.dumps({'args':args,'exit':p.returncode},indent=2)+'\n')
 if p.returncode: raise RuntimeError(name+' failed, receipts retained')
 return p.stdout

sources={'native':'examples/common/bench_produce_settings.rs','c':'benchmarks/peers/librdkafka/peer.c','go':'benchmarks/peers/franz-go/records.go'}
source={k:(R/v).read_text() for k,v in sources.items()}
rust=source['native'][source['native'].index('fn mix('):]
c=source['c'][source['c'].index('static uint64_t mix('):source['c'].index('typedef struct {')]
go=source['go'].replace('package peer','package main',1)
write('native-fragment.rs',rust);write('c-fragment.c',c);write('go-records.go',go)
write('native-harness.rs','''use std::io::{self,BufRead};\nstruct Bytes(Vec<u8>);\nimpl Bytes {fn from(v:Vec<u8>)->Self{Self(v)} fn copy_from_slice(v:&[u8])->Self{Self(v.to_vec())}}\n'''+rust+'''\nfn main(){for line in io::stdin().lock().lines(){let s=line.unwrap();let v:Vec<u64>=s.split_whitespace().map(|x|x.parse().unwrap()).collect();let k=id_key(v[0],v[1]);let p=seeded_payload(v[0],v[1],v[2] as usize);for b in k.0{print!("{b:02x}")}print!(" ");for b in p.0{print!("{b:02x}")}println!();}}\n''')
write('c-harness.c','''#include <stdint.h>\n#include <inttypes.h>\n#include <stdio.h>\n#include <stdlib.h>\n#include <string.h>\n'''+c+'''\nint main(void){uint64_t seed,id;size_t n;while(scanf("%"SCNu64" %"SCNu64" %zu",&seed,&id,&n)==3){unsigned char key[16],*value=malloc(n?n:1);if(!value||n>4096)return 2;record(id,seed,key,value,n,1);for(size_t i=0;i<16;i++)printf("%02x",key[i]);putchar(' ');for(size_t i=0;i<n;i++)printf("%02x",value[i]);putchar('\\n');free(value);}return ferror(stdin)?1:0;}\n''')
write('go-harness.go','''package main\nimport("bufio";"fmt";"os";"strconv";"strings")\nfunc main(){s:=bufio.NewScanner(os.Stdin);w:=bufio.NewWriter(os.Stdout);defer w.Flush();for s.Scan(){f:=strings.Fields(s.Text());seed,e:=strconv.ParseUint(f[0],10,64);if e!=nil{panic(e)};id,e:=strconv.ParseUint(f[1],10,64);if e!=nil{panic(e)};n,e:=strconv.Atoi(f[2]);if e!=nil{panic(e)};fmt.Fprintf(w,"%x %x\\n",MakeKey(id,seed),MakeValue(id,seed,n))};if e:=s.Err();e!=nil{panic(e)}}\n''')
env=os.environ.copy();env.update(RUSTUP_HOME='/workspace/work/rustup',CARGO_HOME='/workspace/work/cargo',GOMAXPROCS='1',GOTOOLCHAIN='local',GOPROXY='off',GOCACHE='/workspace/work/goal-qualification-20261028/go-build')
run('compile-rust',['/workspace/work/cargo/bin/rustc','+stable','--edition=2021','-C','opt-level=2','native-harness.rs','-o','native-harness'],env)
run('compile-c',['cc','-std=c11','-O2','-Wall','-Wextra','-Werror','-Wno-unused-function','c-harness.c','-o','c-harness'],env)
run('compile-go',['/workspace/work/goal-qualification-20261028/go-toolchain/go/bin/go','build','-o','go-harness','go-harness.go','go-records.go'],env)
seeds=[0,1,0x5eed0001,2**64-1];ids=[0,1,2**32-1,2**32,2**63-1,2**63,2**64-2,2**64-1];sizes=[0,1,7,8,9,100,257]
cases=list(itertools.product(seeds,ids,sizes));inputs=''.join(f'{s} {i} {n}\n' for s,i,n in cases);write('vectors.input',inputs)
outputs={name:run('run-'+name,['./'+name+'-harness'],env,inputs).splitlines() for name in ('native','c','go')}
failures=[]
for name,rows in outputs.items():
 if len(rows)!=len(cases): failures.append({'kind':'line_count','driver':name,'got':len(rows),'expected':len(cases)})
for index,case in enumerate(cases):
 if any(index>=len(rows) for rows in outputs.values()): continue
 values={name:rows[index] for name,rows in outputs.items()}
 if len(set(values.values()))!=1: failures.append({'case':case,'index':index,'outputs':values})
 for name,row in values.items():
  parts=row.split(' ')
  if len(parts)!=2 or len(parts[0])!=32 or len(parts[1])!=case[2]*2: failures.append({'case':case,'driver':name,'kind':'length'})
record={'source_commit':subprocess.check_output(['git','-C',str(R),'rev-parse','HEAD'],text=True).strip(), 'source_files':{sources[k]:sha(R/v) for k,v in sources.items()}, 'fragment_sha256':{'native':sha_bytes(rust.encode()),'c':sha_bytes(c.encode()),'go_transformed_package':sha_bytes(go.encode())}, 'harness_source_sha256':{name:sha(D/name) for name in ['native-harness.rs','c-harness.c','go-harness.go','go-records.go']}, 'binary_sha256':{k:sha(D/(k+'-harness')) for k in outputs}, 'input_sha256':sha(D/'vectors.input'), 'output_sha256':{k:sha(D/('run-'+k+'.stdout')) for k in outputs}, 'cases':len(cases),'failures':failures,'scope':'Extracted deterministic generator functions only. Rust uses a byte-container stand-in; Go package declaration is changed for the standalone harness. Size 0 exercises the generator but the native benchmark configuration rejects it. No Kafka broker, actual driver, performance, batch or queue qualification.'}
write('results.json',json.dumps(record,indent=2)+'\n')
print(f'{len(cases)} vectors, {len(failures)} failures; evidence {D}')
if failures:sys.exit(1)

#!/usr/bin/env python3
"""Handwritten pinned-schema controls; these bytes are not Apache executions.

The later genuine SDK oracle checks these finite controls separately. This
source never imports or reads the Rust implementation.
"""
import hashlib,json,pathlib,struct
ROOT=pathlib.Path(__file__).resolve().parent

def string(value):
    return bytes([len(value.encode())+1])+value.encode()

def request(key,version,body):
    client=b'capability-schema'
    return struct.pack('>hhiH',key,version,7,len(client))+client+b'\0'+body

def response(body):
    return struct.pack('>i',7)+b'\0'+body

def write(directory,name,key,version,req,resp,extra):
    directory.mkdir(exist_ok=True)
    files=[]
    for side,data in [('request',request(key,version,req)),('response',response(resp))]:
        p=directory/(name+'.'+side+'.bin');p.write_bytes(data)
        files.append({'file':p.name,'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)})
    return {'name':name,'api_key':key,'api_version':version,'provenance':'handwritten exact official schema control; no actual Apache execution inferred','files':files,**extra}

marker_prefix=b'\2'+struct.pack('>qh?',1000,2,False)+b'\2'+string('t')+b'\2'+struct.pack('>i',0)+b'\0'+struct.pack('>i',7)
marker_response=b'\2'+struct.pack('>q',1000)+b'\2'+string('t')+b'\2'+struct.pack('>ih',0,0)+b'\0\0\0\0'
rows=[]
for tv in [-128,-1,0,1,2,127]:
    req=marker_prefix+struct.pack('>b',tv)+b'\0\0'
    rows.append(write(ROOT,'schema-tv-'+str(tv).replace('-','minus'),27,2,req,marker_response,{'transaction_version':tv}))
(ROOT/'schema-vectors.json').write_text(json.dumps({'schema_version':1,'scope':'reference controls only; actual SDK proof pending','cases':rows},indent=2)+'\n')
share=ROOT.parent/'share-offsets-v1'
req=b'\2'+string('g')+b'\0\0\0'
rows=[]
for lag in [-(1<<63),-2,-1,0,1,(1<<63)-1]:
    body=struct.pack('>i',13)+b'\2'+string('g')+b'\2'+string('t')+bytes([1])*16+b'\2'+struct.pack('>iqiqh',0,17,7,lag,0)+b'\0\0\0'+struct.pack('>h',0)+b'\0\0\0'
    rows.append(write(share,'schema-lag-'+str(lag).replace('-','minus'),90,1,req,body,{'raw_lag':lag}))
(share/'schema-vectors.json').write_text(json.dumps({'schema_version':1,'scope':'reference controls only; actual SDK proof pending','cases':rows},indent=2)+'\n')

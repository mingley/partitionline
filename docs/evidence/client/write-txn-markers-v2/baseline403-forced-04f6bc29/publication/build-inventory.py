from pathlib import Path
import gzip,hashlib,json,os,stat,time
BASE=Path('/workspace/work/client-capability-qa-preparation-e90efb49');PUB=BASE/'publication-baseline403-04f6bc29';PREFIX='docs/evidence/client/write-txn-markers-v2/baseline403-forced-04f6bc29'
sha=lambda data:hashlib.sha256(data).hexdigest();rows=[];excluded=[];seen=set();LIMIT=12*1024*1024
def add(path,rel):
 path=Path(path);st=path.lstat();assert stat.S_ISREG(st.st_mode),(path,'nonregular')
 data=path.read_bytes();assert len(data)<=LIMIT,(path,len(data));assert not data.startswith(b'\x7fELF') and not data.startswith(b'PK\x03\x04')
 assert path.suffix not in {'.rlib','.rmeta','.jar','.class','.so','.o','.a','.bin'},path
 payload_kind='source/text/log/json'
 if path.suffix=='.gz':
  raw=gzip.decompress(data);assert len(raw)<=64*1024*1024;json.loads(raw);payload_kind='gzipJSON_source_audit_only'
 else:
  data.decode('utf-8');assert not data.startswith(b'\xca\xfe\xba\xbe')
 dest=PREFIX+'/'+rel;assert dest not in seen;seen.add(dest)
 rows.append({'source_path':str(path),'proposed_repo_target':dest,'sha256':sha(data),'bytes':len(data),'full_mode':stat.S_IMODE(st.st_mode),'mtime_ns':st.st_mtime_ns,'payload_kind':payload_kind,'source_frozen_or_closed':True})
roots=['baseline-peer-correction-after-first-controls','platform-daemon-exception-preparation','platform-daemon-exception-preparation-v2','cache-tag-protection-preparation','cache-tag-protection-preparation-v4','cache-tag-protection-preparation-v5','package-quarantine-preparation-v1','package-quarantine-preparation-v2',
 'baseline403-forced-04f6bc29-attempt-01','baseline403-forced-04f6bc29-platform-v2-attempt-01','baseline403-forced-04f6bc29-marker-v5-attempt-01','baseline403-forced-04f6bc29-quarantine-v2-attempt-01',
 'stable-baseline403-after-7942-attempt-01','baseline403-full-source-guards-7942','stable-focused-attempt-01','stable-focused-7942fbac-attempt-01','stable-focused-f0ca05da-attempt-01','stable-focused-f0ca05da-attempt-02',
 'runner-preflight-f0ca-attempt-01','helper-correction-after-first-compile','java-only-attempt-01','java-diagnostic-overlay-attempt-02','java-correction-after-first-compile']
for root in roots:
 for p in sorted((BASE/root).rglob('*')):
  if not p.is_file():continue
  if 'retained-elfs' in p.parts or p.suffix in {'.class','.jar','.bin','.rlib','.rmeta','.so','.o','.a'}:
   st=p.stat();excluded.append({'source_path':str(p),'bytes':st.st_size,'sha256':sha(p.read_bytes()),'full_mode':stat.S_IMODE(st.st_mode),'reason':'actualcompiledartifact_or_binaryfixture_WORKonly; publishedrestoremaps/pins retainidentity'});continue
  add(p,'preparation-and-history/'+p.relative_to(BASE).as_posix())
for p in sorted(BASE.iterdir()):
 if p.is_file() and (p.name.startswith('run-') or p.name.startswith('baseline') or p.name.endswith('-driver.log') or p.name in ['qa-plan.json','predeclare-plan.py','forced-baseline-runner-preparation-controls.json','guard-original403-for-baseline.py','focused-incremental-forecast-after-e90-failure.json','source-review.json','review-source.py','warm-cache-after-first-failure.json','cache-after-f0ca-compile-failure.json']):add(p,'preparation-and-history/'+p.name)
add(BASE/'package-invalidation-proposal-after-baseline-setup/proposal.json','preparation-and-history/package-invalidation-proposal-after-baseline-setup/proposal.json')
for name in ['cache-marker-review-01','package-quarantine-review-01']:
 for p in sorted((Path('/workspace/work/integration')/name).rglob('*')):
  if p.is_file():add(p,'independent-review/'+name+'/'+p.relative_to(Path('/workspace/work/integration')/name).as_posix())
for label,origin_path in [('current04f','/workspace/work/integration/client-capabilities-source-04f6bc29/receipt.json'),('original403','/workspace/work/integration/broker-merged-source-403be1e3/receipt.json')]:
 p=Path(origin_path);origin=json.loads(p.read_bytes());add(p,'source-origins/'+label+'/receipt.json');manifest=origin['source_manifest'];p=Path(manifest['path']);data=p.read_bytes();assert sha(data)==manifest['compressed_sha256'];raw=gzip.decompress(data);assert sha(raw)==manifest['uncompressed_sha256'] and len(raw)==manifest['uncompressed_bytes'];add(p,'source-origins/'+label+'/complete-source.json.gz')
# Copy no payloads: original closed files remain canonical inputs and the root
# installer can copy exact bytes/modes into their explicit proposed repo paths.
result={'schema_version':1,'kind':'source_only_actualbaseline_publication_inventory','source_sha':'04f6bc2968c1d721c6815a6389897a62e4ca76f1','baseline_source_sha':'403be1e3db073df86921d6fb21189f695c4f1eaf',
 'proposed_repo_prefix':PREFIX,'all_publication_rows_are_source_or_text_or_JSON_source_audit':True,'binary_compiled_gzip_ELF_rlib_rmeta_JAR_class_payloads_included':False,
 'payload_copy_or_source_repo_mutation_during_preparation':False,'zero_copy_inventory_references_closed_original_WORK_inputs':True,
 'files':rows,'file_count':len(rows),'file_bytes':sum(r['bytes'] for r in rows),'maximum_blob_bytes':max(r['bytes'] for r in rows),'maximum_allowed_blob_bytes':LIMIT,
 'excluded_binary_payloads_WORKonly':excluded,'excluded_file_count':len(excluded),'excluded_byte_count':sum(x['bytes'] for x in excluded),
 'originalraw_quarantine25_WORK_only':'/workspace/work/client-capability-qa-preparation-e90efb49/baseline403-quarantine-04f6bc29-quarantine-v2-attempt-01; approvedoriginal25map791e813 and actualreceipt14a493 bind allbytes/07777/mtime/inodes',
 'newoldgenerated_package25_WORK_only':'final-new-owned-package-map474080eb in currentbaseline run; no rootcachemutation authorized',
 'actual_accepted_proof':{'positive2':True,'API27_red3':True,'API90_red1':True,'availability_old8only_not9':True,'actualoldlibrary_differentbytes_andnmnewlagabsent':True,'sixjoinedcommands':True,'candidate_behavior_orSDK_execution':False},
 'historical_failures_preserved_not_relabelled':'initialRusthelpercompile101/siblingstickycompile101; Java4.1strictcompilefailure; baselinepositiveunsupported19 setup/cachewronglibrary; unreadableDockerprocessguard; invalidCACHEDIRclean101; strictdryrun16of25; unlaunchedquarantineV1postcompilecounterexample',
 'inventory_self_not_payload_row':True,'generator_sha256':sha(Path(__file__).read_bytes()),'generated_at_unix':time.time()}
p=PUB/'publication-inventory.json';assert not p.exists();p.write_text(json.dumps(result,indent=2)+'\n');p.chmod(0o600)
print(json.dumps({'inventory_sha256':sha(p.read_bytes()),'files':len(rows),'total_bytes':result['file_bytes'],'max_blob':result['maximum_blob_bytes'],'excluded_artifact_files':len(excluded),'no_payload_copies':True}))

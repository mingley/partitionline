import ast,difflib,hashlib,json,pathlib,os
os.umask(0o077);R=pathlib.Path(__file__).parent;B=R.with_name('tcp-qualification-02')
g=R/'guard-functions.py';old=g.read_text();tree=ast.parse(old);n=next(x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name=='cache_owners');lines=old.splitlines(keepends=True)
new='''def cache_owners():
    """Refuse extant unreadable processes; optional root-reviewed exact pin only."""
    from process_guards import bounded_docker_empty, cache_references
    pin_rows = ()
    proof = None
    ordinal = len(receipt.get('cache_process_inspections', []))
    guard_write()
    if A.platform_daemon_pin:
        pin_rows, proof = bounded_docker_empty(
            A.platform_daemon_pin, A.platform_daemon_sha256, OUT, ordinal)
    inspection = cache_references('/proc', ENV['CARGO_TARGET_DIR'], os.getpid(), pin_rows)
    inspection['actual_empty_docker_query'] = proof
    receipt.setdefault('cache_process_inspections', []).append(inspection)
    assert not inspection['live_inspection_faults'], (
        'unreadable or replaced live process refused; cache not safe to overwrite',
        inspection['live_inspection_faults'])
    return inspection['owners']
'''
v=''.join(lines[:n.lineno-1])+new+''.join(lines[n.end_lineno:]);ast.parse(v);g.write_text(v)
q=R/'run-next-focused.py';oldrun=q.read_text();v=oldrun
x="P.add_argument('--prior-cache-sha256')";y=x+"\nP.add_argument('--platform-daemon-pin')\nP.add_argument('--platform-daemon-sha256')";assert v.count(x)==1;v=v.replace(x,y)
x="assert hashlib.sha256(driver.with_name('artifact_paths.py').read_bytes()).hexdigest()==prov['artifact_parser_sha256']";y=x+"\nassert hashlib.sha256(driver.with_name('process_guards.py').read_bytes()).hexdigest()==prov['process_guard_sha256']\nassert bool(A.platform_daemon_pin)==bool(A.platform_daemon_sha256), 'exact optional daemon pin requires both path and SHA; default is no exception'";assert v.count(x)==1;v=v.replace(x,y)
x="'source_before':before,'source_after':guards(),'disk_monitor':monitor";y="'source_before':before,'disk_monitor':monitor";assert v.count(x)==1;v=v.replace(x,y)
x="""        item['executed_elfs']=executed_elfs(output,name);item['post_command_elfs']=retain_cache(name,'all success/failure current ELFs',force_verify=True);receipt['all_current_elfs']=item['post_command_elfs']
        retained=preserve_whole_cache('post-'+name);reference_cache_receipt=retained;reference_cache_map=json.loads(Path(retained['map']).read_bytes());item['source_after_retention']=guards();save()"""
y="""        # Preserve all generated objects before source/harness qualification can
        # refuse. A parser failure must never bypass whole-cache/ELF retention.
        item['post_command_elfs']=retain_cache(name,'all success/failure current ELFs BEFORE harness parsing',force_verify=True);receipt['all_current_elfs']=item['post_command_elfs'];save()
        retained=preserve_whole_cache('post-'+name);reference_cache_receipt=retained;reference_cache_map=json.loads(Path(retained['map']).read_bytes());save()""";assert v.count(x)==1;v=v.replace(x,y)
x="""        receipt['actual_capture_bytes']=sum(r['bytes'] for r in item['captures'].values());save()
        if code""";y="""        receipt['actual_capture_bytes']=sum(r['bytes'] for r in item['captures'].values());item['post_command_preservation_completed_before_harness_parsing']=True;save()
        item['source_after']=guards();item['source_after_retention']=item['source_after'];save()
        item['executed_elfs']=executed_elfs(output,name);save()
        if code""";assert v.count(x)==1;v=v.replace(x,y);ast.parse(v);q.write_text(v)
prov=json.loads((R/'guard-provenance.json').read_bytes());prov['derivative_of']={'path':str(B/'guard-provenance.json'),'sha256':hashlib.sha256((B/'guard-provenance.json').read_bytes()).hexdigest()};prov['extracted_sha256']=hashlib.sha256(g.read_bytes()).hexdigest();prov['process_guard_sha256']=hashlib.sha256((R/'process_guards.py').read_bytes()).hexdigest();prov['change']='cache_owners now delegates fail-closed bounded process inspection; executed_elfs keeps prior actual-harness-only parser. All other extracted baseline helper bodies unchanged. Optional exact two-daemon exception requires pinned identity and joined empty actual Docker UNIX API, is not a universal inspection claim.';(R/'guard-provenance.json').write_text(json.dumps(prov,indent=2)+'\n')
plan=json.loads((R/'next-run-plan.json').read_bytes())
for r in plan['candidate_sources']:
 p=R/'candidate'/r['path']
 if p.exists():r.update(source=str(p),bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),full07777=p.stat().st_mode&0o7777)
plan['additive_derivative_of']={'path':str(B/'handoff.json'),'sha256':'e4250889d4cd2e68818a21b19e6a68a8caeb7b516ad54643a24b34880a54591b'};plan['process_ownership']='Default fail-closed inspection with no exception. Any future root-reviewed two-platform-daemon exception must supply exact5b225336 pin, actual joined empty Docker UNIX API before each guard, and retain explicit unreadable field limitation.';plan['direct_owner_receipts']='After actual blocking-owner thread join only: zero constructed owners and peak bounds use actual mpsc max_capacity. No supervisor/listener/network joining claim for these direct helpers.';(R/'next-run-plan.json').write_text(json.dumps(plan,indent=2)+'\n')
for filename,previous,current in [('guard-functions.py.patch',old,g.read_text()),('run-next-focused.py.patch',oldrun,q.read_text())]:
 (R/filename).write_text(''.join(difflib.unified_diff(previous.splitlines(keepends=True),current.splitlines(keepends=True),fromfile=str(B/filename.removesuffix('.patch')),tofile=str(R/filename.removesuffix('.patch')))))
print('Future runner source patched; no runtime execution.')

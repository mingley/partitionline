from pathlib import Path
import ast,json,hashlib,stat
BASE=Path('/workspace/work/client-capability-qa-preparation-e90efb49');P=BASE/'package-quarantine-preparation-v2/check-quarantine-controls.py';raw=P.read_bytes();tree=ast.parse(raw)
stop=next(i for i,n in enumerate(tree.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='receipt' for t in n.targets))
ns={'__file__':str(P)};exec(compile(ast.fix_missing_locations(ast.Module(body=tree.body[:stop],type_ignores=[])),str(P),'exec'),ns)
assert len(ns['RESULTS'])==30
extra=[]
for field in ['mode','mtime','inode','symlink']:
 m=ns['Model']();helpers=m.namespace();helpers['quarantine_own_package_outputs'](m.checked());name=sorted(m.own)[0];q=ns['Q']+'/'+name
 if field=='mode':m.nodes[q]['mode']=stat.S_IFREG|0o644
 elif field=='mtime':m.nodes[q]['mtime']+=1
 elif field=='inode':m.nodes[q]['inode']+=1
 else:m.nodes[q]['mode']=stat.S_IFLNK|0o600
 ns['rejects'](helpers['quarantine_retained_originals_after_compile']);extra.append({'name':'retained-original-'+field+'-change-rejected','passed':True})
m=ns['Model']();helpers=m.namespace();helpers['quarantine_own_package_outputs'](m.checked())
for name,row in m.own.items():m.make(ns['TARGET']+'/'+name,stat.S_IFREG|row['full_mode'],b'x'*row['bytes'])
result=helpers['quarantine_retained_originals_after_compile']();assert result['all25_original_sha_bytes_fullmodes_mtimes_inodes_preserved']
assert all(x['current_cache_path_diagnostic'].get('sha256') and not x['current_cache_path_diagnostic']['identity_matches_expected'] for x in result['retained_originals_and_current_cache_paths'])
extra.append({'name':'same-length-recreated25-hashes-newbytes-originals-preserved','passed':True})
row={'schema_version':1,'scope':'Independent exact extracted in-memory V2 controls; no actual filesystem mutation outside own WORK receipt','runner_sha256':hashlib.sha256((BASE/'run-forced-baseline403-quarantine-v2.py').read_bytes()).hexdigest(),'inherited30controls_independently_pass':True,'controls':ns['RESULTS']+extra,'total':35,'actual_cache_or_quarantine_mutations':0,'actual_Docker_Cargo_or_SDK_commands':0,'inherited_ASTs_unchanged':ns['inherited']}
out=Path(__file__).parent/'v2-controls.json';assert not out.exists();out.write_text(json.dumps(row,indent=2)+'\n');out.chmod(0o600);print(json.dumps({'path':str(out),'sha256':hashlib.sha256(out.read_bytes()).hexdigest(),'bytes':out.stat().st_size,'full_mode':stat.S_IMODE(out.stat().st_mode),'controls':35}))

from pathlib import Path
import ast,json,hashlib,stat
BASE=Path('/workspace/work/client-capability-qa-preparation-e90efb49');P=BASE/'package-quarantine-preparation-v1/check-quarantine-controls.py';raw=P.read_bytes();tree=ast.parse(raw)
stop=next(i for i,n in enumerate(tree.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='receipt' for t in n.targets))
ns={'__file__':str(P)};exec(compile(ast.fix_missing_locations(ast.Module(body=tree.body[:stop],type_ignores=[])),str(P),'exec'),ns)
assert len(ns['RESULTS'])==26
m=ns['Model']();helpers=m.namespace();helpers['quarantine_own_package_outputs'](m.checked())
name=sorted(m.own)[16];source=ns['TARGET']+'/'+name;retained=ns['Q']+'/'+name
original=m.identity(m.nodes[retained]);m.make(source,stat.S_IFREG|0o600,b'genuine-recompiled-new-bytes-model')
assert m.identity(m.nodes[retained])==original
observed=helpers['quarantine_location_snapshot'](list(m.own))
assert observed['all25_present_exactly_once_and_intact'] is False
assert all(m.identity(m.nodes[ns['Q']+'/'+n])=={k:row[k] for k in ['sha256','bytes','full_mode','mtime_ns','inode']} for n,row in m.own.items())
row={'schema_version':1,'scope':'Independent extracted in-memory V1 quarantine controls only; no actual cache/quarantine/runtime operation','runner_sha256':hashlib.sha256((BASE/'run-forced-baseline403-quarantine-v1.py').read_bytes()).hexdigest(),'inherited26controls_independently_pass':True,'inherited_ASTs_unchanged':ns['inherited'],'counterexample':{'retained_quarantine25_all_original_identities_match':True,'new_cache_artifact_same_original_path_different_bytes':True,'posttest_original_XOR_guard_accepts':observed['all25_present_exactly_once_and_intact'],'requires_retained_only_postcompile_check':True,'sampled_original':original,'observed_source':m.identity(m.nodes[source])},'actual_operational_commands':0,'actual_cache_or_quarantine_mutations':0}
out=Path(__file__).parent/'v1-controls-and-counterexample.json';assert not out.exists();out.write_text(json.dumps(row,indent=2)+'\n');out.chmod(0o600);print(json.dumps({'path':str(out),'sha256':hashlib.sha256(out.read_bytes()).hexdigest(),'bytes':out.stat().st_size,'full_mode':stat.S_IMODE(out.stat().st_mode)}))

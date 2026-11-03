#!/usr/bin/env python3
"""Build a candidate-only inventory pin update and qualification overlay."""
import difflib, hashlib, json
from pathlib import Path
w=Path('/workspace/work/broker-join-inventory-investigation-20261103')
r=Path('/workspace/work/broker-join-inventory-r2-20261103')
checkout=Path('/workspace/partitionline')
def sha(b): return hashlib.sha256(b).hexdigest()
def replace_once(s,a,b):
 assert s.count(a)==1,(a,s.count(a))
 return s.replace(a,b,1)
spec=json.loads((r/'spec.json').read_text())
manifest=json.loads((r/'base-files.json').read_text())
registry='tests/conformance/broker/implemented-api-versions.json'
pre=(w/'source'/registry).read_bytes()
assert sha(pre)==manifest[registry]['sha256']
old=spec['join_base_source']['sha256'];new=spec['join_fixed_source']['sha256']
needle=('"controller_source_sha256": "'+old+'"').encode()
replacement=('"controller_source_sha256": "'+new+'"').encode()
assert pre.count(needle)==1
post=pre.replace(needle,replacement,1)
left=json.loads(pre);right=json.loads(post)
assert left['controller_source']==spec['join_source_path']
left['controller_source_sha256']=new
assert left==right
patch=''.join(difflib.unified_diff(pre.decode().splitlines(keepends=True),post.decode().splitlines(keepends=True),fromfile='a/'+registry,tofile='b/'+registry)).encode()
(r/'join-registry.patch').write_bytes(patch)
spec['join_registry_path']=registry
spec['join_registry_base_source']=manifest[registry]
spec['join_registry_fixed_source']={'sha256':sha(post),'bytes':len(post),'mode':manifest[registry]['mode']}
spec['join_registry_patch']={'path':'join-registry.patch','sha256':sha(patch),'bytes':len(patch)}
spec['limits'].append('The candidate-only registry overlay updates only the controller source hash to the exact fixed join source; the hosted r2 qualification has not run.')
(r/'spec.json').write_text(json.dumps(spec,indent=2)+'\n')
s=(r/'qualify.py').read_text()
s=replace_once(s,"SPEC_SHA = '232f4cae75d7ba6efe0f69058ac787df089bdbc44b4daee605ef4bd5a9f6df4b'","SPEC_SHA = '"+sha((r/'spec.json').read_bytes())+"'")
s=replace_once(s,"                    'join_baseline_patch','join_fixed_patch','join_receipt','join_independent_review',\n", "                    'join_baseline_patch','join_fixed_patch','join_registry_patch','join_receipt','join_independent_review',\n")
s=replace_once(s,"        require(base[protocol] == spec['join_base_source'], 'join preimage differs from base catalog')\n", "        require(base[protocol] == spec['join_base_source'], 'join preimage differs from base catalog')\n        registry = spec['join_registry_path']\n        require(registry == 'tests/conformance/broker/implemented-api-versions.json' and\n                base[registry] == spec['join_registry_base_source'], 'registry preimage differs from base catalog')\n")
s=replace_once(s,"        fixed = dict(capacity_fixed); fixed[protocol] = spec['join_fixed_source']\n", "        join_fixed = dict(capacity_fixed); join_fixed[protocol] = spec['join_fixed_source']\n        fixed = dict(join_fixed); fixed[registry] = spec['join_registry_fixed_source']\n")
s=replace_once(s,"        legacy.extract_source(assets['base_source'], source, base);legacy.verify_source(source,base)\n", "        legacy.extract_source(assets['base_source'], source, base);legacy.verify_source(source,base)\n        require(legacy.load(source/registry)['controller_source_sha256'] == spec['join_base_source']['sha256'],\n                'base registry controller source pin differs')\n")
s=replace_once(s,"        apply(assets['join_fixed_patch'])\n        legacy.restore_patched_source_modes(source,fixed,{protocol},output/'fixed-source-modes.json')\n", "        apply(assets['join_fixed_patch'])\n        legacy.restore_patched_source_modes(source,join_fixed,{protocol},output/'join-fixed-source-modes.json')\n        apply(assets['join_registry_patch'])\n        legacy.restore_patched_source_modes(source,fixed,{registry},output/'fixed-source-modes.json')\n        require(legacy.load(source/registry)['controller_source_sha256'] == spec['join_fixed_source']['sha256'],\n                'fixed registry controller source pin differs')\n")
(r/'qualify.py').write_text(s)
s=(r/'test_static.py').read_text()
s=replace_once(s,"for key in ('join_baseline_patch','join_fixed_patch','join_receipt','join_independent_review','workflow_preimage','capacity_wrapper_preimage'):","for key in ('join_baseline_patch','join_fixed_patch','join_registry_patch','join_receipt','join_independent_review','workflow_preimage','capacity_wrapper_preimage'):")
s=replace_once(s,"run_static(verified['test_static.py'], str(root/base/'test_static.py'), 11)","run_static(verified['test_static.py'], str(root/base/'test_static.py'), 12)")
assert s.count('controller-join-v2-r1')==2
s=s.replace('controller-join-v2-r1','controller-join-v2-r2')
marker='    def test_join_negative_parser_rejects_wrong_or_unrelated_failures(self):\n'
assert s.count(marker)==1
newtest='''    def test_registry_patch_only_rebinds_reviewed_fixed_controller_source(self):
        name=self.spec['join_registry_path']
        self.assertEqual(name,'tests/conformance/broker/implemented-api-versions.json')
        self.assertEqual(self.manifest[name],self.spec['join_registry_base_source'])
        with tarfile.open(HERE/'base-source.tar.gz','r:gz') as archive:
            before=archive.extractfile(name).read()
        self.assertEqual(hashlib.sha256(before).hexdigest(),self.manifest[name]['sha256'])
        checkout=os.environ.get('CAPACITY_CHECKOUT') or os.environ.get('GITHUB_WORKSPACE')
        scratch=detached_scratch_base(os.environ.get('RUNNER_TEMP') or tempfile.gettempdir(),checkout)
        with tempfile.TemporaryDirectory(prefix='join-registry-',dir=scratch) as temp:
            root=Path(temp);target=root/name;target.parent.mkdir(parents=True);target.write_bytes(before)
            patch=HERE/self.spec['join_registry_patch']['path']
            for flags in (['--check'],[]):
                subprocess.run(['git','apply',*flags,str(patch)],cwd=root,check=True,capture_output=True,timeout=120,env=clean_git_env())
            after=target.read_bytes();meta=self.spec['join_registry_fixed_source']
            self.assertEqual((len(after),hashlib.sha256(after).hexdigest()),(meta['bytes'],meta['sha256']))
            old=json.loads(before);new=json.loads(after)
            self.assertEqual(old['controller_source'],self.spec['join_source_path'])
            self.assertEqual(old['controller_source_sha256'],self.spec['join_base_source']['sha256'])
            old['controller_source_sha256']=self.spec['join_fixed_source']['sha256']
            self.assertEqual(new,old)
            for flags in (['--reverse','--check'],['--reverse']):
                subprocess.run(['git','apply',*flags,str(patch)],cwd=root,check=True,capture_output=True,timeout=120,env=clean_git_env())
            self.assertEqual(target.read_bytes(),before)

'''
s=s.replace(marker,newtest+marker,1)
(r/'test_static.py').write_text(s)
wf=(checkout/'.github/workflows/candidates.yml').read_text()
assert sha(wf.encode())=='09fc1b57f3e2dc21562fecb35354d0e14c5b2ae63e1567c904e6d82fd9671b00'
assert wf.count('controller-join-v2-r1')==4
newwf=wf.replace('controller-join-v2-r1','controller-join-v2-r2')
for name in ('qualify.py','test_static.py','spec.json'):
 oldp={'qualify.py':('32cf64d1a3d718714ee23d6d1e501aa0d20240fc8817d37c160897f4e69deef2',19066),
       'test_static.py':('487ac493339cd3b4d35c6d0081d8a9873df5792bb5282b6bc20edbb2cf1a27fc',26347),
       'spec.json':('232f4cae75d7ba6efe0f69058ac787df089bdbc44b4daee605ef4bd5a9f6df4b',7613)}[name]
 data=(r/name).read_bytes();newwf=replace_once(newwf,f"'{name}': ('{oldp[0]}', {oldp[1]})",f"'{name}': ('{sha(data)}', {len(data)})")
newwf=replace_once(newwf,"run_static(verified['test_static.py'], str(root/base/'test_static.py'), 11)","run_static(verified['test_static.py'], str(root/base/'test_static.py'), 12)")
(r/'workflow-r2.yml').write_text(newwf)
workflow_patch=''.join(difflib.unified_diff(wf.splitlines(keepends=True),newwf.splitlines(keepends=True),fromfile='a/.github/workflows/candidates.yml',tofile='b/.github/workflows/candidates.yml'))
(r/'candidates-workflow.patch').write_text(workflow_patch)
(r/'registry-preimage.json').write_bytes(pre)
(r/'registry-postimage.json').write_bytes(post)
print(json.dumps({'registry_pre_sha256':sha(pre),'registry_post_sha256':sha(post),'registry_patch_sha256':sha(patch),
 'qualify_sha256':sha((r/'qualify.py').read_bytes()),'static_sha256':sha((r/'test_static.py').read_bytes()),
 'spec_sha256':sha((r/'spec.json').read_bytes()),'workflow_pre_sha256':sha(wf.encode()),'workflow_post_sha256':sha(newwf.encode()),
 'workflow_patch_sha256':sha(workflow_patch.encode())},indent=2))

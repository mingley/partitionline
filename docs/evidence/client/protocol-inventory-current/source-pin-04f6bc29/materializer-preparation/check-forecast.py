#!/usr/bin/env python3
"""Read-only exact JSON/gzip forecast checks; never execute materialization."""
import ast
import gzip
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import zlib

HERE = Path(__file__).parent
ORIGINAL = Path('/workspace/work/raft-runtime-76/materializer-preparation-01/materialize-from-baseline.py')
CANDIDATE = HERE/'materialize-from-baseline-exact-audit.py'
REPO = Path('/workspace/partitionline')
SOURCE = '04f6bc2968c1d721c6815a6389897a62e4ca76f1'
BASE = '7942fbac784b35220ae02f6983e6454399f42ba5'
ORIGIN = Path('/workspace/work/integration/client-capabilities-source-7942fbac/receipt.json')
ORIGIN_SHA = '6b32de0a48931c1a7fcb85ae15165661f9303f501e5936c17c36cb1c570f4007'
FLOOR = 350*1024*1024
MAX_BLOB = 32*1024*1024
MAX_MANIFEST = 128*1024*1024


def require(condition, label):
    if not condition:
        raise ValueError(label)


def binding(path):
    s = path.lstat()
    require(stat.S_ISREG(s.st_mode), 'regular frozen input')
    return {'path':str(path),'bytes':s.st_size,'full_mode':s.st_mode & 0o7777,
            'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}


def git(*args, data=None):
    return subprocess.check_output(['git',*args],cwd=REPO,input=data)


def metadata(commit):
    require(git('rev-parse','--verify',commit+'^{commit}').decode().strip() == commit,'exact current Git pin')
    raw = git('ls-tree','-r','-z',commit)
    tree = {}
    for row in raw.split(b'\0'):
        if not row:
            continue
        meta, path = row.split(b'\t',1)
        mode, kind, blob = meta.decode().split()
        name = path.decode()
        require(kind == 'blob' and mode in ('100644','100755') and name not in tree,'bounded exact regular Git entries')
        require(not Path(name).is_absolute() and all(part not in ('','.','..') for part in name.split('/')),'unchanged Git path policy')
        require(len(tree) < 1_000_000,'entry bound')
        tree[name] = (mode,blob)
    objects = sorted({blob for _,blob in tree.values()})
    checks = git('cat-file','--batch-check=%(objectname) %(objecttype) %(objectsize)',
                 data=''.join(blob+'\n' for blob in objects).encode())
    sizes = {}
    for row in checks.decode().splitlines():
        blob,kind,value = row.split();size = int(value)
        require(kind == 'blob' and 0 <= size <= MAX_BLOB and blob not in sizes,'exact object sizes and unchanged cap')
        sizes[blob] = size
    require(set(sizes) == set(objects),'complete metadata-only size enumeration')
    return tree,sizes,{'ls_tree_sha256':hashlib.sha256(raw).hexdigest(),
                       'batch_check_sha256':hashlib.sha256(checks).hexdigest(),'file_count':len(tree),
                       'unique_blob_count':len(objects),'Git_blob_payloads_read':0}


def analytic_length(entries,sizes,mode_for):
    # Assemble exact JSON punctuation/types independently of dict-dump ordering.
    # Only the path needs JSON string escaping. Other strings have fixed ASCII
    # alphabets and widths; int fields have exact decimal digits.
    length = 2 + max(0,len(entries)-1)
    for name,(mode,blob) in entries.items():
        body = ('{"bytes":'+str(sizes[blob])+',"full_permission_mode":'+str(mode_for(mode))
                +',"git_blob_sha1":"'+blob+'","mode":"'+mode+'","sha256":"'+'f'*64+'"}')
        length += len(json.dumps(name).encode()) + 1 + len(body.encode())
    return length


def check_case(exact,entries,sizes,mode_for):
    predicted = exact(entries,sizes,mode_for)
    independent = analytic_length(entries,sizes,mode_for)
    # Distinct valid 64hex digests and reversed input/field insertion orders
    # exercise all metadata without pretending these are actual file SHA256s.
    alternate = {}
    for name in reversed(list(entries)):
        mode,blob = entries[name]
        alternate[name] = {'sha256':hashlib.sha256((name+blob).encode('utf-8','surrogatepass')).hexdigest(),
                           'full_permission_mode':mode_for(mode),'bytes':sizes[blob],
                           'git_blob_sha1':blob,'mode':mode}
    actual = json.dumps(alternate,sort_keys=True,separators=(',',':')).encode()
    require(predicted == independent == len(actual),'exact sorted compact JSON length equality')
    return predicted,hashlib.sha256(actual).hexdigest()


def main():
    require(os.sched_getaffinity(0) == {2,4},'Python-only CPU2,4')
    originals = [binding(ORIGINAL),binding(CANDIDATE),binding(ORIGIN)]
    require(originals[0]['sha256'] == 'c9a31c31d6a145c2309536c711bb268341753d77da9a7dc84cea41acb7cf3a0d'
            and originals[2]['sha256'] == ORIGIN_SHA,'original c9 and7942 origin unchanged')
    old, new = ORIGINAL.read_text(), CANDIDATE.read_text()
    first = 'audit_upper = sum(len(name.encode()) * 6 + 384 for name in entries) + 2\n'
    end = 'guard_write(2 * block)\n'
    require(old.split(first)[0] == new.split('def exact_audit_length(')[0],'unchanged driver before forecast')
    require(old.split(end,1)[1] == new.split(end,1)[1].replace('assert len(encoded) == audit_upper',
                                                          'assert len(encoded) <= audit_upper'),
            'all materialization/guards/writes/compression/retention unchanged except exact final length check')
    old_ast,new_ast = ast.parse(old),ast.parse(new)
    preserved = []
    for name in ('guard_write','git_blob','git_tree','worktree_inodes','verify_base'):
        a = next(n for n in old_ast.body if isinstance(n,ast.FunctionDef) and n.name == name)
        b = next(n for n in new_ast.body if isinstance(n,ast.FunctionDef) and n.name == name)
        require(ast.dump(a,include_attributes=False) == ast.dump(b,include_attributes=False),'unchanged guard AST '+name)
        preserved.append(name)
    funcs = [n for n in new_ast.body if isinstance(n,ast.FunctionDef) and n.name in ('exact_audit_length','stored_gzip_upper')]
    require(len(funcs) == 2,'only forecast helpers extracted; never execute driver top-level')
    context = {'json':json,'MAX_MANIFEST':MAX_MANIFEST}
    exec(compile(ast.Module(body=funcs,type_ignores=[]),str(CANDIDATE),'exec'),context)
    exact,bound = context['exact_audit_length'],context['stored_gzip_upper']
    origin = json.loads(ORIGIN.read_bytes())
    require(origin['source_commit'] == BASE,'7942 explicit origin pin')
    manifest = Path(origin['source_manifest']['path'])
    compressed = manifest.read_bytes();raw = gzip.decompress(compressed)
    require(hashlib.sha256(compressed).hexdigest() == origin['source_manifest']['compressed_sha256']
            and hashlib.sha256(raw).hexdigest() == origin['source_manifest']['uncompressed_sha256']
            and len(raw) == origin['source_manifest']['uncompressed_bytes'],'pinned full baseline metadata')
    originals.append(binding(manifest))
    base_pin = json.loads(raw)
    base_tree,base_sizes,base_meta = metadata(BASE)
    require(set(base_pin) == set(base_tree) and len(base_pin) == origin['verified_files'],'complete baseline metadata pathset')
    for name,pin in base_pin.items():
        require((pin['mode'],pin['git_blob_sha1']) == base_tree[name]
                and pin['bytes'] == base_sizes[pin['git_blob_sha1']],'baseline exact Git modes/blob/sizes')
    entries,sizes,current_meta = metadata(SOURCE)
    mode_for = lambda mode: 0o700 if mode == '100755' else 0o600
    exact_bytes,alternate_hash = check_case(exact,entries,sizes,mode_for)
    require(exact_bytes <= MAX_MANIFEST,'unchanged128MiB manifest cap')
    controls = []
    names = ['ascii','quote"slash\\','tabs\tnewlines\nreturns\r','control\x01\x1f','café/用户/💫',
             'separators\u2028\u2029','del\x7f','long/'+'x'*16384,'literal-escape\\u0000','surrogate\ud800']
    widths = [0,1,9,10,99,100,999,1000,9999,10000,MAX_BLOB-1,MAX_BLOB]
    modes = [0,1,9,10,63,64,99,100,384,448,4095]
    for index,name in enumerate(names):
        for size in widths:
            for permission in modes:
                blob = ('f' if index & 1 else '0')*40
                test = {name:('100755' if index & 1 else '100644',blob)}
                predicted,_ = check_case(exact,test,{blob:size},lambda mode,p=permission:p)
                controls.append({'path_case':index,'size':size,'fullmode':permission,'exact_bytes':predicted})
    check_case(exact,{}, {},mode_for)
    all_path_entries = {name:('100644',str(i).zfill(40)) for i,name in enumerate(names)}
    all_path_sizes = {blob:widths[i % len(widths)] for i,(_,blob) in enumerate(all_path_entries.values())}
    check_case(exact,all_path_entries,all_path_sizes,mode_for)
    controls_sha = hashlib.sha256(json.dumps(controls,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    # Mathematical domain proof:5*ceil(n/16383) dominates the default zlib
    # n>>12+n>>14 terms; n>>25<=4 at128MiB, and64 wrapper slack dominates25+4.
    boundary_sizes = {0,1,MAX_MANIFEST,MAX_MANIFEST-1,exact_bytes}
    for step in (4096,16383,16384,33554432):
        for multiple in range(0,MAX_MANIFEST//step+1):
            for delta in (-1,0,1):
                n = multiple*step+delta
                if 0 <= n <= MAX_MANIFEST:
                    boundary_sizes.add(n)
    for n in boundary_sizes:
        require(bound(n) >= n+(n>>12)+(n>>14)+(n>>25)+25,'stored-block bound dominates default zlib gzip bound')
    gzip_tests = []
    for n in (0,1,4095,4096,16382,16383,16384,65536,1024*1024):
        for pattern in ('zero','counter-hash'):
            payload = b'0'*n if pattern == 'zero' else b''.join(hashlib.sha256(str(i).encode()).digest() for i in range((n+31)//32))[:n]
            output = gzip.compress(payload,mtime=0)
            require(len(output) <= bound(n) and gzip.decompress(output) == payload,'actual unchanged gzip settings within rigorous bound')
            gzip_tests.append({'bytes':n,'pattern':pattern,'actual_gzip_bytes':len(output),'upper_bound':bound(n)})
    v=os.statvfs('/workspace/work');block=max(v.f_frsize,4096)
    rounded=lambda n:((n+block-1)//block)*block
    reusable={name for name,(mode,blob) in entries.items() if (pin:=base_pin.get(name))
              and pin['mode']==mode and pin['git_blob_sha1']==blob and pin['full_permission_mode']==mode_for(mode)}
    new_bytes=sum(sizes[blob] for name,(_,blob) in entries.items() if name not in reusable)
    new_allocated=sum(rounded(sizes[blob]) for name,(_,blob) in entries.items() if name not in reusable)
    directories={str(parent) for name in entries for parent in Path(name).parents if str(parent)!='.'}
    entry_reserve=(len(entries)+len(directories)+2)*block
    gz_upper=bound(exact_bytes)
    forecast=new_allocated+entry_reserve+rounded(exact_bytes)+rounded(gz_upper)+1_048_576
    old_upper=sum(len(name.encode())*6+384 for name in entries)+2
    old_gzip=old_upper+((old_upper+16382)//16383)*5+64
    old_forecast=new_allocated+entry_reserve+2*old_upper+old_gzip+1_048_576
    current_after=metadata(SOURCE)[2]
    require(current_after==current_meta,'exact Git metadata unchanged')
    require(all(binding(Path(b['path']))==b for b in originals),'all frozen source/origin/manifest inputs unchanged')
    available=v.f_bavail*v.f_frsize
    receipt={'schema_version':1,'passed':True,'scope':'WORK-only metadata forecast and adversarial controls.0 materializations,0 Cargo/SDK/runtime,0 Git mutation or cleanup. Baseline file-content verification remains the unchanged driver execution guard, not a new claim here.',
             'source_sha':SOURCE,'baseline_sha':BASE,'actual_affinity':[2,4],'current_Git_metadata':current_meta,
             'baseline_Git_metadata':base_meta,'frozen_inputs':originals,'guard_functions_identical_AST':preserved,
             'all_nonforecast_driver_bytes_identical_except_stricter_final_length_assert':True,
             'all_actual_metadata_JSON_length_equality':True,'exact_current_audit_bytes':exact_bytes,
             'alternate_64hex_projection_SHA256':alternate_hash,'adversarial_length_cases':len(controls)+2,
             'adversarial_case_definitions':{'path_cases':names[:-1]+['surrogate U+D800 (serializer-domain only, not accepted Git path)'],
                                            'blob_bytes':widths,'fullmode_decimal_values':modes},
             'adversarial_controls_SHA256':controls_sha,'gzip_integer_boundary_checks':len(boundary_sizes),
             'gzip_actual_cases':gzip_tests,'Python_zlib_build_version':zlib.ZLIB_VERSION,'Python_zlib_runtime_version':zlib.ZLIB_RUNTIME_VERSION,
             'gzip_proof':'For0<=n<=128MiB:5*ceil(n/16383)>=floor(n/4096)+floor(n/16384);floor(n/33554432)<=4;64>25+4. Thus stored-block upper also dominates unchanged Python default-zlib gzip bound. Empirical gzip controls are supplemental, not the proof.',
             'forecast':{'new_blob_bytes':new_bytes,'new_allocated_file_upper_bound_bytes':new_allocated,
                         'hardlink_and_directory_entry_reserve_bytes':entry_reserve,'block_bytes':block,
                         'directory_count':len(directories),'source_file_count':len(entries),'reusable_file_count':len(reusable),
                         'raw_audit_exact_encoded_bytes':exact_bytes,'raw_audit_allocated_upper_bound_bytes':rounded(exact_bytes),
                         'gzip_upper_bound_bytes':gz_upper,'gzip_allocated_upper_bound_bytes':rounded(gz_upper),
                         'receipt_and_minimal_metadata_reserve_bytes':1_048_576,'forecast_output_bytes':forecast,
                         'required_free_bytes':FLOOR+forecast,'sampled_free_bytes':available,'minimum_remaining_free_bytes':FLOOR,
                         'read_only_snapshot_would_allow_launch':available>=FLOOR+forecast,
                         'old_forecast_output_bytes':old_forecast,'old_required_free_bytes':FLOOR+old_forecast,
                         'exact_forecast_reduction_bytes':old_forecast-forecast},
             'driver':binding(CANDIDATE),'patch':binding(HERE/'forecast-only.patch'),'controls_source':binding(Path(__file__))}
    out=HERE/'forecast-validation.json';require(not out.exists(),'fresh evidence output')
    out.write_text(json.dumps(receipt,indent=2)+'\n');out.chmod(0o600)
    print(json.dumps({'passed':True,'forecast':receipt['forecast'],'validation_sha256':binding(out)['sha256']}),flush=True)


if __name__=='__main__':
    main()
